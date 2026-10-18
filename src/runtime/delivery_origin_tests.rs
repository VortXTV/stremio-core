//! SAVED/UNRUN D1 v3 fixtures. Actual Runtime, Ctx reducers and Env futures.
//! Frozen v2 d11 remains the direct-send negative control; this successor must be GREEN.
//! Authority issuance below is an explicit TEST seam, NOT actual Owner/Unknown or Ctx policy.
use super::*;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread;

use chrono::{TimeZone, Utc};
use futures::{future, Future, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::models::ctx::{Ctx, CtxStatus};
use crate::models::streaming_server::StreamingServer;
use crate::runtime::msg::CtxAuthResponse;
use crate::runtime::{
    CompletionEvents, CompletionPublisher, CompletionSubmission, ConditionalSend,
    DeliveryAuthority, DeliveryLease, DeliveryLeaseError, EnvError, EnvFuture, EnvFutureExt,
    LocalPrefix, PendingCompletion, PrefixCompletion, ScopeToken, TryEnvFuture, Update,
};
use crate::types::api::AuthRequest;
use crate::types::profile::{Auth, AuthKey, User};

thread_local! {
    static SEQUENTIAL: RefCell<VecDeque<EnvFuture<'static, ()>>> = RefCell::new(VecDeque::new());
    static CONCURRENT: RefCell<VecDeque<EnvFuture<'static, ()>>> = RefCell::new(VecDeque::new());
    static READY: RefCell<Option<mpsc::Sender<()>>> = RefCell::new(None);
    static WRITES: Cell<usize> = Cell::new(0);
    static SIBLINGS: Cell<usize> = Cell::new(0);
    static FAIL_STORAGE: Cell<bool> = Cell::new(false);
}
struct DeliveryEnv;
impl Env for DeliveryEnv {
    fn fetch<
        IN: Serialize + ConditionalSend + 'static,
        OUT: for<'de> Deserialize<'de> + ConditionalSend + 'static,
    >(
        _: http::Request<IN>,
    ) -> TryEnvFuture<OUT> {
        panic!("auth fetch futures are intentionally not polled; no network fixture")
    }
    fn get_storage<T: for<'de> Deserialize<'de> + ConditionalSend + 'static>(
        _: &str,
    ) -> TryEnvFuture<Option<T>> {
        panic!("no hydration")
    }
    fn set_storage<T: Serialize>(_: &str, value: Option<&T>) -> TryEnvFuture<()> {
        let encoded = value.map(serde_json::to_vec).transpose();
        async move {
            encoded.map_err(EnvError::from)?;
            WRITES.with(|count| count.set(count.get() + 1));
            // Signal AFTER the actual producer future completed its simulated write,
            // before Runtime's callback attempts transition. Never wait in this future.
            READY.with(|ready| {
                if let Some(tx) = ready.borrow_mut().take() {
                    tx.send(()).unwrap();
                }
            });
            if FAIL_STORAGE.with(Cell::get) {
                Err(EnvError::StorageWriteError("synthetic".into()))
            } else {
                Ok(())
            }
        }
        .boxed_env()
    }
    fn exec_concurrent<F: Future<Output = ()> + ConditionalSend + 'static>(future: F) {
        CONCURRENT.with(|queue| queue.borrow_mut().push_back(future.boxed_env()));
    }
    fn exec_sequential<F: Future<Output = ()> + ConditionalSend + 'static>(future: F) {
        SEQUENTIAL.with(|queue| queue.borrow_mut().push_back(future.boxed_env()));
    }
    fn now() -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(100, 0).unwrap()
    }
    fn flush_analytics() -> EnvFuture<'static, ()> {
        future::ready(()).boxed_env()
    }
    fn analytics_context(_: &Ctx, _: &StreamingServer, _: &str) -> serde_json::Value {
        serde_json::Value::Null
    }
    #[cfg(debug_assertions)]
    fn log(_: String) {
        panic!("no logging")
    }
}
fn reset() {
    SEQUENTIAL.with(|queue| queue.borrow_mut().clear());
    CONCURRENT.with(|queue| queue.borrow_mut().clear());
    READY.with(|ready| drop(ready.borrow_mut().take()));
    WRITES.with(|count| count.set(0));
    SIBLINGS.with(|count| count.set(0));
    FAIL_STORAGE.with(|fail| fail.set(false));
}
fn sequential_one() {
    let task = SEQUENTIAL
        .with(|queue| queue.borrow_mut().pop_front())
        .expect("sequential task");
    futures::executor::block_on(task);
}
fn concurrent_one() {
    let task = CONCURRENT
        .with(|queue| queue.borrow_mut().pop_front())
        .expect("completion waiter");
    futures::executor::block_on(task);
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Field {
    Ctx,
}
struct OriginalAuthority {
    storage: ScopeToken,
    attempt: ScopeToken,
}
type PendingSlot = Arc<Mutex<Option<(CompletionPublisher, CompletionEvents)>>>;
struct DropWatch(Box<dyn Fn() + Send + Sync>);
impl Drop for DropWatch {
    fn drop(&mut self) {
        (self.0)();
    }
}
struct Harness {
    ctx: Ctx,
    storage: ScopeToken,
    attempt: ScopeToken,
    original: ScopeToken,
    deferred: bool,
    capture_next: bool,
    pending: PendingSlot,
    detached: bool,
    root_captures: Arc<AtomicU64>,
    checks: Arc<Mutex<Vec<bool>>>,
    authority: Arc<DeliveryAuthority>,
    closing: Arc<std::sync::atomic::AtomicBool>,
    drop_watch: Option<DropWatch>,
}
impl Clone for Harness {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            storage: self.storage.clone(),
            attempt: self.attempt.clone(),
            original: self.original.clone(),
            deferred: self.deferred,
            capture_next: false,
            pending: Arc::clone(&self.pending),
            detached: true,
            root_captures: Arc::clone(&self.root_captures),
            checks: Arc::clone(&self.checks),
            authority: Arc::clone(&self.authority),
            closing: Arc::clone(&self.closing),
            drop_watch: None,
        }
    }
}
impl Harness {
    fn new(deferred: bool) -> Self {
        let storage = ScopeToken::new(());
        let attempt = ScopeToken::new(());
        let original = ScopeToken::new(OriginalAuthority {
            storage: storage.clone(),
            attempt: attempt.clone(),
        });
        let mut ctx = Ctx::default();
        ctx.profile.auth = Some(auth("A"));
        Self {
            ctx,
            storage,
            attempt,
            original,
            deferred,
            capture_next: true,
            pending: Arc::new(Mutex::new(None)),
            detached: false,
            root_captures: Arc::new(AtomicU64::new(0)),
            checks: Arc::new(Mutex::new(vec![])),
            authority: Arc::new(DeliveryAuthority::new()),
            closing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drop_watch: None,
        }
    }
    fn current(&self, token: &ScopeToken) -> bool {
        !self.detached
            && token
                .get::<OriginalAuthority>()
                .map(|old| {
                    old.storage.same_token(&self.storage) && old.attempt.same_token(&self.attempt)
                })
                .unwrap_or(false)
    }
    fn refresh_test_authority(&mut self) {
        // Test-only issuer, called solely by actual Model update under transition.
        // This is not a proposed production Ctx token/counter implementation.
        self.original = ScopeToken::new(OriginalAuthority {
            storage: self.storage.clone(),
            attempt: self.attempt.clone(),
        });
    }
}
impl Model<DeliveryEnv> for Harness {
    type Field = Field;
    fn update(&mut self, msg: &Msg) -> (Vec<Effect>, Vec<Field>) {
        assert!(!self.detached);
        let effects = Update::<DeliveryEnv>::update(&mut self.ctx, msg);
        let fields = if effects.has_changed {
            vec![Field::Ctx]
        } else {
            vec![]
        };
        (effects.into_iter().collect(), fields)
    }
    fn update_field(&mut self, msg: &Msg, _: &Field) -> (Vec<Effect>, Vec<Field>) {
        self.update(msg)
    }
    fn supports_recorded_dispatch(&self) -> bool {
        !self.detached
    }
    fn root_scope(&self) -> Option<ScopeToken> {
        self.root_captures.fetch_add(1, Ordering::SeqCst);
        Some(self.original.clone())
    }
    fn completion_lease(&self, original: &ScopeToken) -> Result<DeliveryLease<'_>, ScopeDrop> {
        let lease = self.authority.acquire().map_err(|_| ScopeDrop::Foreign)?;
        let current = self.current(original) && !self.closing.load(Ordering::SeqCst);
        self.checks.lock().unwrap().push(current); // TEST authority, not private Owner
        if current {
            Ok(lease)
        } else {
            Err(ScopeDrop::StaleAttempt)
        }
    }
    fn update_field_scoped(
        &mut self,
        msg: &Msg,
        _: &Field,
        scope: &EffectScope,
    ) -> ScopedPlan<Field> {
        self.update_scoped(msg, scope)
    }
    fn update_scoped(&mut self, msg: &Msg, scope: &EffectScope) -> ScopedPlan<Field> {
        if !scope.origin().map(|old| self.current(old)).unwrap_or(false) {
            return ScopedPlan::reject(
                scope,
                AdmissionOutcome::DroppedScope(ScopeDrop::StaleOrigin),
            );
        }
        // Exercise actual Ctx reducers but supply authority at the explicitly injected seam.
        if matches!(msg, Msg::Action(Action::Ctx(ActionCtx::Authenticate(_)))) {
            self.attempt = ScopeToken::new(()); // even the SAME request value is a new attempt
            self.refresh_test_authority();
        }
        let adopts = matches!((msg, &self.ctx.status),
            (Msg::Internal(Internal::CtxAuthResult(request, Ok(_))), CtxStatus::Loading(loading)) if request == loading);
        if adopts || matches!(msg, Msg::Internal(Internal::Logout(_))) {
            self.storage = ScopeToken::new(()); // A -> B -> A never reuses storage identity
            self.refresh_test_authority();
        }
        let effects = Update::<DeliveryEnv>::update(&mut self.ctx, msg);
        let fields = if effects.has_changed {
            vec![Field::Ctx]
        } else {
            vec![]
        };
        let next_scope = EffectScope::recorded(self.original.clone());
        let mut plan = ScopedPlan::ordinary(vec![], fields, scope);
        if self.capture_next && matches!(msg, Msg::Internal(Internal::ProfileChanged)) {
            self.capture_next = false;
            let mut effects = effects.into_iter();
            let Some(Effect::Future(EffectFuture::Sequential(actual))) = effects.next() else {
                panic!(
                    "pinned actual ProfileChanged producer must return its real sequential future"
                )
            };
            assert!(effects.next().is_none());
            let deferred = self.deferred;
            let holder = Arc::clone(&self.pending);
            plan.prefix = Some(LocalPrefix {
                origin: self.original.clone(),
                future: async move {
                    let Msg::Event(event) = actual.await else {
                        panic!("actual producer must return Event")
                    };
                    let events = [Some(event), None];
                    if deferred {
                        let (publisher, receiver) = PendingCompletion::channel();
                        let mut holder = holder.lock().unwrap();
                        assert!(
                            holder.is_none(),
                            "a later Unresolved must not replace original publisher"
                        );
                        *holder = Some((publisher, events));
                        drop(holder); // no holder/backend/owner guard reaches Runtime
                        PrefixCompletion::Pending(receiver)
                    } else {
                        PrefixCompletion::Final(events)
                    }
                }
                .boxed_env(),
            });
            for id in ["before", "after"] {
                plan.effects.push(ScopedEffect::ordinary(
                    Effect::Msg(Box::new(Msg::Event(Event::NotificationsDismissed {
                        id: id.into(),
                    }))),
                    next_scope.clone(),
                ));
            }
            plan.effects.push(ScopedEffect::ordinary(
                Effect::Future(EffectFuture::Sequential(
                    async {
                        SIBLINGS.with(|count| count.set(count.get() + 1));
                        Msg::Event(Event::NotificationsDismissed {
                            id: "fifo-sibling".into(),
                        })
                    }
                    .boxed_env(),
                )),
                next_scope,
            ));
        } else {
            plan.effects = effects
                .into_iter()
                .map(|effect| ScopedEffect::ordinary(effect, next_scope.clone()))
                .collect();
        }
        plan
    }
}
type TestRuntime = Runtime<DeliveryEnv, Harness>;
type TestRx = RecordedReceiver<RuntimeEvent<DeliveryEnv, Harness>>;
fn auth(id: &str) -> Auth {
    Auth {
        key: AuthKey("synthetic-key".into()),
        user: User {
            id: id.into(),
            ..Default::default()
        },
    }
}
fn request() -> AuthRequest {
    AuthRequest::LoginWithToken {
        token: "synthetic-request".into(),
    }
}
fn new_runtime(deferred: bool) -> (TestRuntime, TestRx, PendingSlot, ScopeToken) {
    let model = Harness::new(deferred);
    let pending = Arc::clone(&model.pending);
    let original = model.original.clone();
    let decline: DeclineReceiver = Arc::new(|_| {});
    let (runtime, rx) = Runtime::new_recorded(
        model,
        vec![],
        EffectScope::recorded(original.clone()),
        decline,
    )
    .unwrap_or_else(|_| panic!("recorded construction"));
    (runtime, rx, pending, original)
}
fn begin(runtime: &TestRuntime, original: &ScopeToken, fenced: bool) {
    runtime.handle_scheduled_effect_output(
        Msg::Internal(Internal::ProfileChanged),
        EffectScope::recorded(original.clone()),
        AddonEffectContext { epoch: 0, fenced },
    );
}
fn core_events<S: Stream<Item = RuntimeEvent<DeliveryEnv, Harness>> + Unpin>(
    rx: &mut S,
) -> Vec<Event> {
    let mut events = vec![];
    while let Some(Some(output)) = rx.next().now_or_never() {
        if let RuntimeEvent::CoreEvent(event) = output {
            events.push(event);
        }
    }
    events
}
fn storage_event(event: &Event) -> bool {
    match event {
        Event::ProfilePushedToStorage { .. } => true,
        Event::Error { source, .. } => storage_event(source),
        _ => false,
    }
}
fn action_locked(runtime: &TestRuntime, action: ActionCtx) {
    // Caller holds the actual Runtime transition. Use its actual plan/pair/traversal.
    let msg = Msg::Action(Action::Ctx(action));
    let (planned, scope) = runtime.plan_action_scoped(&msg, None);
    let context = planned.addon_context;
    let direct = runtime.handle_invocation(planned, &msg, &scope);
    runtime.finish_bare(msg, scope, direct, context);
}
fn adopt_locked(runtime: &TestRuntime, id: &str) {
    action_locked(runtime, ActionCtx::Authenticate(request()));
    let scope = EffectScope::recorded(runtime.model.read().unwrap().original.clone());
    let msg = Msg::Internal(Internal::CtxAuthResult(
        request(),
        Ok(CtxAuthResponse {
            auth: auth(id),
            addons_result: Ok(vec![]),
            library_items_result: Ok(vec![]),
        }),
    ));
    runtime.handle_effect_output_scoped(
        msg,
        scope,
        AddonEffectContext {
            epoch: runtime.addon_epoch.load(Ordering::SeqCst),
            fenced: false,
        },
    );
}

#[derive(Clone, Copy)]
enum Change {
    None,
    Switch,
    Aba,
    SameRequestRetry,
    AddonOnly,
}
fn race(deferred: bool, change: Change, fail: bool, fenced: bool) {
    reset();
    let (created_tx, created_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let (ready_tx, ready_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        reset();
        FAIL_STORAGE.with(|slot| slot.set(fail));
        let (runtime, rx, pending, original) = new_runtime(deferred);
        begin(&runtime, &original, fenced);
        if deferred {
            sequential_one(); // returns Pending without awaiting resolution
            sequential_one(); // genuine later FIFO sibling runs before resolution
            assert_eq!(SIBLINGS.with(Cell::get), 1);
        }
        created_tx
            .send((runtime.clone(), rx, original.clone()))
            .unwrap();
        go_rx.recv().unwrap(); // outside transition/model/backend guards
        assert!(
            runtime
                .model
                .read()
                .unwrap()
                .completion_lease(&original)
                .is_ok(),
            "future-side precheck is still true BEFORE the forced transition change"
        );
        if deferred {
            let (publisher, events) = pending.lock().unwrap().take().unwrap();
            assert_eq!(publisher.submit(events), CompletionSubmission::Queued);
            ready_tx.send(()).unwrap();
            concurrent_one();
        } else {
            READY.with(|ready| *ready.borrow_mut() = Some(ready_tx));
            sequential_one();
            sequential_one();
        }
        (WRITES.with(Cell::get), SIBLINGS.with(Cell::get))
    });
    let (runtime, mut rx, original) = created_rx.recv().unwrap();
    let transition = runtime.lock_transition();
    go_tx.send(()).unwrap();
    ready_rx.recv().unwrap(); // only before-transition readiness; NEVER wait for callback completion
    match change {
        Change::None => {}
        Change::Switch => adopt_locked(&runtime, "B"),
        Change::Aba => {
            adopt_locked(&runtime, "B");
            adopt_locked(&runtime, "A");
        }
        Change::SameRequestRetry => {
            action_locked(&runtime, ActionCtx::Authenticate(request()));
            action_locked(&runtime, ActionCtx::Authenticate(request()));
            assert!(
                matches!(&runtime.model.read().unwrap().ctx.status, CtxStatus::Loading(value) if value == &request())
            );
        }
        Change::AddonOnly => {
            let addons = runtime.model.read().unwrap().ctx.profile.addons.clone();
            action_locked(&runtime, ActionCtx::ReplaceAddonProjectionLocal(addons));
        }
    }
    let captures = runtime
        .model
        .read()
        .unwrap()
        .root_captures
        .load(Ordering::SeqCst);
    let current = runtime.model.read().unwrap().current(&original);
    let should_deliver = current && (!fenced || !matches!(change, Change::AddonOnly));
    drop(transition);
    assert_eq!(worker.join().unwrap(), (1, 1)); // ONLY after transition release
    assert_eq!(
        runtime
            .model
            .read()
            .unwrap()
            .root_captures
            .load(Ordering::SeqCst),
        captures,
        "completion must not recapture a current root"
    );
    let events = core_events(&mut rx);
    let completions: Vec<_> = events.iter().filter(|event| storage_event(event)).collect();
    assert_eq!(completions.len(), usize::from(should_deliver));
    if let Some(event) = completions.first() {
        if fail {
            assert!(
                matches!(event, Event::Error { source, .. } if matches!(source.as_ref(), Event::ProfilePushedToStorage { uid } if uid.as_ref().map(|id| id.as_ref()) == Some("A")))
            );
        } else {
            assert!(
                matches!(event, Event::ProfilePushedToStorage { uid } if uid.as_ref().map(|id| id.as_ref()) == Some("A"))
            );
        }
    }
    for id in ["before", "after"] {
        assert_eq!(events.iter().filter(|event| matches!(event, Event::NotificationsDismissed { id: actual } if actual == id)).count(), 1);
    }
    // Its work always ran once. An initial-path FIFO sibling delivered after addon
    // invalidation still obeys the EXISTING addon output fence; D1 does not waive it.
    let fifo_visible = deferred || !fenced || !matches!(change, Change::AddonOnly);
    assert_eq!(events.iter().filter(|event| matches!(event, Event::NotificationsDismissed { id } if id == "fifo-sibling")).count(), usize::from(fifo_visible));
    if !current {
        assert_eq!(
            runtime.model.read().unwrap().checks.lock().unwrap().last(),
            Some(&false)
        );
    }
    reset(); // discard deliberately unpolled auth/network and later auth storage effects
}

#[test]
fn delivery_origin_d01_initial_transition_races() {
    for change in [Change::Switch, Change::Aba, Change::SameRequestRetry] {
        race(false, change, false, true);
    }
}
#[test]
fn delivery_origin_d02_resolved_transition_races_without_fifo_parking() {
    for change in [Change::Switch, Change::Aba, Change::SameRequestRetry] {
        race(true, change, false, true);
    }
}
#[test]
fn delivery_origin_d03_original_addon_context_survives_resolution() {
    for deferred in [false, true] {
        race(deferred, Change::AddonOnly, false, true);
        race(deferred, Change::AddonOnly, false, false); // preserve existing unfenced semantics
    }
}
#[test]
fn delivery_origin_d04_original_actual_success_and_error_subject_once() {
    for deferred in [false, true] {
        for fail in [false, true] {
            race(deferred, Change::None, fail, true);
        }
    }
}
#[test]
fn delivery_origin_d05_publisher_drop_and_runtime_shutdown_cancel_without_ack() {
    for shutdown in 0..3 {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(true);
        begin(&runtime, &original, true);
        sequential_one();
        sequential_one();
        let (publisher, events) = pending.lock().unwrap().take().unwrap();
        match shutdown {
            0 => {
                drop(publisher); // retirement/cancellation is not an ACK
                concurrent_one();
                drop(runtime);
            }
            1 => {
                drop(runtime);
                concurrent_one();
                assert_eq!(publisher.submit(events), CompletionSubmission::ReceiverGone);
            }
            _ => {
                runtime.cancel_pending_completions(); // works while a Runtime handle still lives
                concurrent_one();
                assert_eq!(publisher.submit(events), CompletionSubmission::ReceiverGone);
                drop(runtime);
            }
        }
        assert_eq!(WRITES.with(Cell::get), 1);
        assert_eq!(
            core_events(&mut rx)
                .iter()
                .filter(|event| storage_event(event))
                .count(),
            0
        );
    }
    reset();
}

#[derive(Clone)]
struct NoDeliveryValidator(Harness);
impl Model<DeliveryEnv> for NoDeliveryValidator {
    type Field = Field;
    fn update(&mut self, msg: &Msg) -> (Vec<Effect>, Vec<Field>) {
        self.0.update(msg)
    }
    fn update_field(&mut self, msg: &Msg, field: &Field) -> (Vec<Effect>, Vec<Field>) {
        self.0.update_field(msg, field)
    }
    fn supports_recorded_dispatch(&self) -> bool {
        self.0.supports_recorded_dispatch()
    }
    fn root_scope(&self) -> Option<ScopeToken> {
        self.0.root_scope()
    }
    fn update_scoped(&mut self, msg: &Msg, scope: &EffectScope) -> ScopedPlan<Field> {
        self.0.update_scoped(msg, scope)
    }
    fn update_field_scoped(
        &mut self,
        msg: &Msg,
        field: &Field,
        scope: &EffectScope,
    ) -> ScopedPlan<Field> {
        self.0.update_field_scoped(msg, field, scope)
    }
    // Intentionally inherits the new public contract's default denial.
}
#[test]
fn delivery_origin_d06_missing_validator_fails_closed_after_prefix_work() {
    reset();
    let model = NoDeliveryValidator(Harness::new(false));
    let scope = EffectScope::recorded(model.0.original.clone());
    let (runtime, mut rx) = Runtime::new_recorded(model, vec![], scope.clone(), Arc::new(|_| {}))
        .unwrap_or_else(|_| panic!("construction"));
    runtime.handle_scheduled_effect_output(
        Msg::Internal(Internal::ProfileChanged),
        scope,
        AddonEffectContext {
            epoch: 0,
            fenced: false,
        },
    );
    sequential_one();
    sequential_one();
    assert_eq!(WRITES.with(Cell::get), 1); // admitted prefix was not discarded
    assert_eq!(SIBLINGS.with(Cell::get), 1);
    let mut completions = 0;
    while let Some(Some(output)) = rx.next().now_or_never() {
        if let RuntimeEvent::CoreEvent(event) = output {
            completions += usize::from(storage_event(&event));
        }
    }
    assert_eq!(completions, 0);
    reset();
}

fn actual_storage_future(ctx: &mut Ctx, msg: Msg) -> EnvFuture<'static, Msg> {
    let mut effects = Update::<DeliveryEnv>::update(ctx, &msg).into_iter();
    let Some(Effect::Future(EffectFuture::Sequential(future))) = effects.next() else {
        panic!("actual storage producer")
    };
    assert!(effects.next().is_none());
    future
}
#[test]
fn delivery_origin_d07_two_logical_actual_producer_events_are_each_validated_once() {
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, _, original) = new_runtime(deferred);
        let (profile, library) = {
            let mut model = runtime.model.write().unwrap();
            let profile =
                actual_storage_future(&mut model.ctx, Msg::Internal(Internal::ProfileChanged));
            let library = actual_storage_future(
                &mut model.ctx,
                Msg::Internal(Internal::LibraryChanged(false)),
            );
            (profile, library)
        };
        let (publisher, receiver) = PendingCompletion::channel();
        let saved = Arc::new(Mutex::new(None));
        let capture = Arc::clone(&saved);
        let mut plan = ScopedPlan::empty(&EffectScope::recorded(original.clone()));
        plan.prefix = Some(LocalPrefix {
            origin: original,
            future: async move {
                let (Msg::Event(profile), Msg::Event(library)) =
                    future::join(profile, library).await
                else {
                    panic!("actual events")
                };
                let events = [Some(profile), Some(library)];
                if deferred {
                    *capture.lock().unwrap() = Some(events);
                    PrefixCompletion::Pending(receiver)
                } else {
                    PrefixCompletion::Final(events)
                }
            }
            .boxed_env(),
        });
        {
            let _transition = runtime.lock_transition();
            runtime.handle_effects(PlannedEffects {
                plan,
                addon_context: AddonEffectContext {
                    epoch: 0,
                    fenced: false,
                },
            });
        }
        sequential_one();
        if deferred {
            assert_eq!(
                publisher.submit(saved.lock().unwrap().take().unwrap()),
                CompletionSubmission::Queued
            );
            concurrent_one();
        } else {
            drop(publisher);
        }
        assert_eq!(WRITES.with(Cell::get), 3); // one Profile + two physical Library members
        let events = core_events(&mut rx);
        assert_eq!(events.len(), 2); // but only two logical completion events
        assert!(
            matches!(&events[0], Event::ProfilePushedToStorage { uid } if uid.as_ref().map(|id| id.as_ref()) == Some("A"))
        );
        assert!(matches!(&events[1], Event::LibraryItemsPushedToStorage { ids } if ids.is_empty()));
        assert_eq!(
            &*runtime.model.read().unwrap().checks.lock().unwrap(),
            &[true, true]
        );
        assert_eq!(
            runtime
                .model
                .read()
                .unwrap()
                .root_captures
                .load(Ordering::SeqCst),
            0
        );
        assert!(core_events(&mut rx).is_empty());
    }
    reset();
}
#[test]
fn delivery_origin_d08_true_legacy_storage_path_does_not_require_new_validator() {
    reset();
    let model = Harness::new(false);
    let (runtime, mut rx) = Runtime::new(model, vec![], 128);
    runtime.handle_scheduled_effect_output(
        Msg::Internal(Internal::ProfileChanged),
        EffectScope::Legacy,
        AddonEffectContext {
            epoch: 0,
            fenced: false,
        },
    );
    sequential_one();
    assert_eq!(
        core_events(&mut rx)
            .iter()
            .filter(|event| storage_event(event))
            .count(),
        1
    );
    assert!(runtime
        .model
        .read()
        .unwrap()
        .checks
        .lock()
        .unwrap()
        .is_empty());
    reset();
}

#[test]
fn delivery_origin_d09_lease_is_retained_through_each_real_commit() {
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(deferred);
        let authority = Arc::clone(&runtime.model.read().unwrap().authority);
        let transition = Arc::downgrade(&runtime.transition);
        let model = Arc::downgrade(&runtime.model);
        let count = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&count);
        *runtime
            .completion_lifetime
            .as_ref()
            .unwrap()
            .send_probe
            .lock()
            .unwrap() = Some(Arc::new(move || {
            assert!(matches!(
                authority.try_acquire(),
                Err(DeliveryLeaseError::Busy)
            ));
            assert!(matches!(
                transition.upgrade().unwrap().try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            assert!(matches!(
                model.upgrade().unwrap().try_write(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        begin(&runtime, &original, true);
        sequential_one();
        if deferred {
            let (publisher, events) = pending.lock().unwrap().take().unwrap();
            assert_eq!(publisher.submit(events), CompletionSubmission::Queued);
            concurrent_one();
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(
            core_events(&mut rx)
                .iter()
                .filter(|event| storage_event(event))
                .count(),
            1
        );
        reset();
    }
}

#[test]
fn delivery_origin_d10_send_failure_unwinds_only_after_releasing_guards() {
    for deferred in [false, true] {
        reset();
        let (runtime, rx, pending, original) = new_runtime(deferred);
        let authority = Arc::clone(&runtime.model.read().unwrap().authority);
        begin(&runtime, &original, true);
        if deferred {
            sequential_one();
        }
        drop(rx); // fail ONLY the completion send; initial siblings already enqueued
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if deferred {
                let (publisher, events) = pending.lock().unwrap().take().unwrap();
                assert_eq!(publisher.submit(events), CompletionSubmission::Queued);
                concurrent_one();
            } else {
                sequential_one();
            }
        }));
        assert!(failed.is_err());
        assert!(
            runtime.transition.try_lock().is_ok(),
            "send panic poisoned transition"
        );
        assert!(
            runtime.model.try_write().is_ok(),
            "send panic retained/poisoned model"
        );
        assert!(
            authority.try_acquire().is_ok(),
            "send panic retained/poisoned authority"
        );
        assert_eq!(WRITES.with(Cell::get), 1);
        reset(); // drops queued unrelated work after all guards, not in the failed send
    }
}

#[test]
fn delivery_origin_d11_recorded_receiver_wake_must_not_run_under_authority() {
    struct WakeAudit {
        authority: Arc<DeliveryAuthority>,
        held: Mutex<Vec<bool>>,
    }
    impl futures::task::ArcWake for WakeAudit {
        fn wake_by_ref(this: &Arc<Self>) {
            let held = matches!(this.authority.try_acquire(), Err(DeliveryLeaseError::Busy));
            this.held.lock().unwrap().push(held);
            // Real arbitrary receiver code could instead call dispatch, cancel or
            // retirement here and deadlock. This witness records without blocking.
        }
    }
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(deferred);
        let authority = Arc::clone(&runtime.model.read().unwrap().authority);
        begin(&runtime, &original, true);
        let _already_sent_siblings = core_events(&mut rx);
        let audit = Arc::new(WakeAudit {
            authority,
            held: Mutex::new(vec![]),
        });
        let waker = futures::task::waker(Arc::clone(&audit));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(matches!(
            futures::Stream::poll_next(std::pin::Pin::new(&mut rx), &mut context),
            std::task::Poll::Pending
        ));
        sequential_one();
        if deferred {
            let (publisher, events) = pending.lock().unwrap().take().unwrap();
            assert_eq!(publisher.submit(events), CompletionSubmission::Queued);
            concurrent_one();
        }
        let observed = audit.held.lock().unwrap();
        assert!(
            !observed.is_empty(),
            "actual output receiver waker was not exercised"
        );
        assert!(
            observed.iter().all(|held| !held),
            "D1 SOURCE BLOCKER: public receiver wake occurred while authority was held"
        );
        drop(observed);
        reset();
    }
}

#[test]
fn delivery_origin_d12_last_upgraded_model_is_destroyed_after_outer_guards() {
    let (created_tx, created_rx) = mpsc::channel();
    let (start_tx, start_rx) = mpsc::channel();
    let (inside_tx, inside_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let dropped = Arc::new(AtomicU64::new(0));
    let worker = thread::spawn(move || {
        reset();
        let (runtime, rx, _pending, original) = new_runtime(false);
        // Only the weak-target prefix is queued here. A normal sibling future
        // would itself retain a Runtime clone and mask the last-model-Arc seam.
        let actual = actual_storage_future(
            &mut runtime.model.write().unwrap().ctx,
            Msg::Internal(Internal::ProfileChanged),
        );
        let scope = EffectScope::recorded(original.clone());
        let mut plan = ScopedPlan::empty(&scope);
        plan.prefix = Some(LocalPrefix {
            origin: original,
            future: async move {
                let Msg::Event(event) = actual.await else {
                    panic!("actual ProfileChanged event")
                };
                PrefixCompletion::Final([Some(event), None])
            }
            .boxed_env(),
        });
        {
            let _transition = runtime.lock_transition();
            runtime.handle_effects(PlannedEffects {
                plan,
                addon_context: AddonEffectContext {
                    epoch: 0,
                    fenced: true,
                },
            });
        }
        created_tx.send((runtime.clone(), rx)).unwrap();
        drop(runtime); // only main's handle remains; callback target is Weak
        start_rx.recv().unwrap();
        sequential_one();
        reset();
    });
    let (runtime, mut rx) = created_rx.recv().unwrap();
    let transition = Arc::downgrade(&runtime.transition);
    let authority = Arc::clone(&runtime.model.read().unwrap().authority);
    let seen = Arc::clone(&dropped);
    runtime.model.write().unwrap().drop_watch = Some(DropWatch(Box::new(move || {
        assert!(
            transition.upgrade().unwrap().try_lock().is_ok(),
            "last model died under transition"
        );
        assert!(
            authority.try_acquire().is_ok(),
            "last model died under authority"
        );
        seen.fetch_add(1, Ordering::SeqCst);
    })));
    let release_rx = Arc::new(Mutex::new(release_rx));
    *runtime
        .completion_lifetime
        .as_ref()
        .unwrap()
        .send_probe
        .lock()
        .unwrap() = Some(Arc::new(move || {
        inside_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap(); // TEST-only; main only drops its Runtime handle
    }));
    start_tx.send(()).unwrap();
    inside_rx.recv().unwrap();
    drop(runtime);
    release_tx.send(()).unwrap();
    worker.join().unwrap(); // no Runtime/Model/authority guard on this thread
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        core_events(&mut rx)
            .iter()
            .filter(|event| storage_event(event))
            .count(),
        1
    );
}

#[test]
fn delivery_origin_d13_external_leaf_closing_denies_initial_and_resolved_delivery() {
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(deferred);
        let (authority, closing) = {
            let model = runtime.model.read().unwrap();
            (Arc::clone(&model.authority), Arc::clone(&model.closing))
        };
        begin(&runtime, &original, true);
        if deferred {
            sequential_one();
        }
        {
            // TEST leaf transition: no model write/Runtime lock. The actual private
            // Owner Closing producer is a separate source dependency, not this bool.
            let _lease = authority.acquire().unwrap();
            closing.store(true, Ordering::SeqCst);
        }
        if deferred {
            let (publisher, events) = pending.lock().unwrap().take().unwrap();
            assert_eq!(publisher.submit(events), CompletionSubmission::Queued);
            concurrent_one();
        } else {
            sequential_one();
        }
        sequential_one(); // unrelated sequential sibling still executes once
        assert_eq!(WRITES.with(Cell::get), 1);
        assert_eq!(SIBLINGS.with(Cell::get), 1);
        assert_eq!(
            core_events(&mut rx)
                .iter()
                .filter(|event| storage_event(event))
                .count(),
            0
        );
        assert_eq!(
            runtime.model.read().unwrap().checks.lock().unwrap().last(),
            Some(&false)
        );
        reset();
    }
}

#[path = "delivery_queue_tests.rs"]
mod delivery_queue_tests;
