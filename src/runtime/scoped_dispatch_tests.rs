//! Phase-one TRANSPORT fixtures, UNRUN. Actual shared action constructor + actual Ctx + Runtime.
//! Admission is injected at the Model seam; this is NOT the private Owner/Unknown E01 gate.
//! Place as runtime/scoped_dispatch_tests.rs and include as a child of runtime.rs.
use super::*;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread;

use chrono::{TimeZone, Utc};
use futures::{future, Future, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::models::ctx::{plan_library_action, Ctx, CtxError, LibraryActionPlan, OtherError};
use crate::models::streaming_server::StreamingServer;
use crate::runtime::{
    AdmissionError, AdmissionReason, ConditionalSend, EnvError, EnvFuture, EnvFutureExt,
    LocalPrefix, PrefixCompletion, ScopeToken, TryEnvFuture, Update,
};
use crate::types::library::{LibraryItem, LibraryItemState};
use crate::types::notifications::NotificationItem;
use crate::types::resource::MetaItemPreview;

thread_local! {
    static QUEUE: RefCell<VecDeque<EnvFuture<'static, ()>>> = RefCell::new(VecDeque::new());
    static CONSTRUCTED: RefCell<Vec<String>> = RefCell::new(Vec::new());
    static EXECUTED: RefCell<Vec<String>> = RefCell::new(Vec::new());
    static PLANNED_CHILDREN: Cell<usize> = Cell::new(0);
}

struct RecordingEnv;
impl Env for RecordingEnv {
    fn fetch<
        IN: Serialize + ConditionalSend + 'static,
        OUT: for<'de> Deserialize<'de> + ConditionalSend + 'static,
    >(
        _: http::Request<IN>,
    ) -> TryEnvFuture<OUT> {
        panic!("network is forbidden in this synthetic transport fixture")
    }
    fn get_storage<T: for<'de> Deserialize<'de> + ConditionalSend + 'static>(
        _: &str,
    ) -> TryEnvFuture<Option<T>> {
        panic!("hydration is outside the transport fixture")
    }
    fn set_storage<T: Serialize>(key: &str, value: Option<&T>) -> TryEnvFuture<()> {
        // Deliberately records the real LEGACY constructor in accepted controls.
        // Phase two must replace all three group constructor sites, not this Env.
        let bytes = value.map(serde_json::to_vec).transpose();
        CONSTRUCTED.with(|keys| keys.borrow_mut().push(key.to_owned()));
        let key = key.to_owned();
        async move {
            bytes.map_err(EnvError::from)?;
            EXECUTED.with(|trace| trace.borrow_mut().push(format!("storage:{key}")));
            Ok(())
        }
        .boxed_env()
    }
    fn exec_concurrent<F: Future<Output = ()> + ConditionalSend + 'static>(future: F) {
        QUEUE.with(|queue| queue.borrow_mut().push_back(future.boxed_env()));
    }
    fn exec_sequential<F: Future<Output = ()> + ConditionalSend + 'static>(future: F) {
        QUEUE.with(|queue| queue.borrow_mut().push_back(future.boxed_env()));
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
        panic!("no payload logging")
    }
}

fn reset_env() {
    QUEUE.with(|queue| queue.borrow_mut().clear());
    CONSTRUCTED.with(|keys| keys.borrow_mut().clear());
    EXECUTED.with(|trace| trace.borrow_mut().clear());
    PLANNED_CHILDREN.with(|count| count.set(0));
}
fn drain_queue() {
    loop {
        let task = QUEUE.with(|queue| queue.borrow_mut().pop_front());
        match task {
            Some(task) => futures::executor::block_on(task),
            None => break,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Field {
    Ctx,
}

struct ActualCtxHarness {
    ctx: Ctx,
    token: ScopeToken,
    outcomes: Arc<Mutex<VecDeque<AdmissionOutcome>>>,
    desugar: bool,
    prefix_and_sentinels: bool,
    detached: bool,
    fault: Option<PlanFault>,
    fault_at_root: bool,
    child_scope: Option<EffectScope>,
}
impl Clone for ActualCtxHarness {
    fn clone(&self) -> Self {
        // Only Runtime's cfg(test) state snapshot needs this copy. It cannot mutate.
        Self {
            ctx: self.ctx.clone(),
            token: self.token.clone(),
            outcomes: Arc::clone(&self.outcomes),
            desugar: self.desugar,
            prefix_and_sentinels: self.prefix_and_sentinels,
            detached: true,
            fault: self.fault,
            fault_at_root: self.fault_at_root,
            child_scope: self.child_scope.clone(),
        }
    }
}
impl ActualCtxHarness {
    fn new(outcome: Option<AdmissionOutcome>) -> Self {
        let mut ctx = Ctx::default();
        ctx.library.items.insert("item".into(), item());
        Self {
            ctx,
            token: ScopeToken::new(()),
            outcomes: Arc::new(Mutex::new(outcome.into_iter().collect())),
            desugar: false,
            prefix_and_sentinels: false,
            detached: false,
            fault: None,
            fault_at_root: false,
            child_scope: None,
        }
    }
    fn actual(&mut self, msg: &Msg, scope: &EffectScope) -> ScopedPlan<Field> {
        let effects = Update::<RecordingEnv>::update(&mut self.ctx, msg);
        let fields = if effects.has_changed {
            vec![Field::Ctx]
        } else {
            vec![]
        };
        ScopedPlan::ordinary(effects.into_iter().collect(), fields, scope)
    }
}
impl Model<RecordingEnv> for ActualCtxHarness {
    type Field = Field;
    fn update(&mut self, msg: &Msg) -> (Vec<Effect>, Vec<Field>) {
        assert!(!self.detached, "snapshot cannot mutate");
        let effects = Update::<RecordingEnv>::update(&mut self.ctx, msg);
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
        Some(self.token.clone())
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
        if self.detached {
            return reject_for(
                msg,
                scope,
                AdmissionOutcome::ProtocolRejected(ProtocolRejection::Detached),
            );
        }
        if !scope
            .origin()
            .map(|token| token.same_token(&self.token))
            .unwrap_or(false)
        {
            return reject_for(
                msg,
                scope,
                AdmissionOutcome::DroppedScope(ScopeDrop::Foreign),
            );
        }
        if self.fault_at_root && matches!(msg, Msg::Action(_)) {
            let mut plan = faulty_plan(&self.ctx, scope, self.fault.take().unwrap());
            // Root general invalid-scope case: legitimate subject class, wrong scope.
            plan.direct.kind = DirectKind::Other {
                outcome: AdmissionOutcome::AcceptedChange,
                error_source: None,
            };
            return plan;
        }
        if let Some(action) = plan_library_action::<RecordingEnv>(&self.ctx.library, msg) {
            let mut plan = ScopedPlan::empty(scope);
            match action {
                LibraryActionPlan::Denied(event) => {
                    plan.direct.kind = DirectKind::Other {
                        outcome: AdmissionOutcome::DomainDenied,
                        error_source: None,
                    };
                    plan.effects.push(ScopedEffect::ordinary(
                        Effect::Msg(Box::new(Msg::Event(event))),
                        scope.descendant(),
                    ));
                }
                LibraryActionPlan::Update(frame) => {
                    let child_scope = self
                        .child_scope
                        .clone()
                        .unwrap_or_else(|| scope.descendant());
                    if self.desugar {
                        plan.effects.extend(
                            frame
                                .into_legacy()
                                .into_iter()
                                .map(|effect| ScopedEffect::ordinary(effect, child_scope.clone())),
                        );
                    } else {
                        plan.effects.push(ScopedEffect::library(frame, child_scope));
                    }
                    if self.prefix_and_sentinels {
                        surround(&mut plan, scope);
                    }
                }
            }
            return plan;
        }
        if let Msg::Internal(Internal::UpdateLibraryItem(item)) = msg {
            PLANNED_CHILDREN.with(|count| count.set(count.get() + 1));
            if let Some(fault) = self.fault.take() {
                return faulty_plan(&self.ctx, scope, fault);
            }
            // INJECTED transport disposition. No owner/intent/conservation assertion here.
            let injected = self.outcomes.lock().unwrap().pop_front();
            let outcome = injected.unwrap_or_else(|| {
                if self
                    .ctx
                    .library
                    .items
                    .get(&item.id)
                    .map(|current| item.eq_no_mtime(current))
                    .unwrap_or(false)
                {
                    AdmissionOutcome::AcceptedNoChange
                } else {
                    AdmissionOutcome::AcceptedChange
                }
            });
            let mut plan = match outcome {
                AdmissionOutcome::AcceptedChange | AdmissionOutcome::AcceptedNoChange => {
                    self.actual(msg, scope)
                }
                _ => ScopedPlan::empty(scope),
            };
            plan.direct = DirectOutcome::library(scope, item.id.clone(), outcome);
            return plan;
        }
        let mut plan = self.actual(msg, scope);
        if self.prefix_and_sentinels
            && matches!(
                msg,
                Msg::Action(Action::Ctx(ActionCtx::DismissNotificationItem(_)))
            )
        {
            surround(&mut plan, scope);
        }
        plan
    }
}

fn surround(plan: &mut ScopedPlan<Field>, scope: &EffectScope) {
    plan.prefix = Some(LocalPrefix {
        origin: scope.origin().expect("recorded fixture").clone(),
        future: async {
            EXECUTED.with(|trace| trace.borrow_mut().push("prefix".into()));
            PrefixCompletion::Final([None, None])
        }
        .boxed_env(),
    });
    plan.effects.insert(0, sentinel("before", scope));
    plan.effects.push(sentinel("after", scope));
}

fn sentinel(name: &'static str, scope: &EffectScope) -> ScopedEffect {
    let future = async move {
        EXECUTED.with(|trace| trace.borrow_mut().push(name.into()));
        Msg::Event(Event::NotificationsDismissed {
            id: format!("sentinel-{name}"),
        })
    }
    .boxed_env();
    ScopedEffect::ordinary(
        Effect::Future(EffectFuture::Sequential(future)),
        scope.descendant(),
    )
}
fn item() -> LibraryItem {
    LibraryItem {
        id: "item".into(),
        name: "Synthetic".into(),
        r#type: "series".into(),
        poster: None,
        poster_shape: Default::default(),
        removed: false,
        temp: false,
        ctime: None,
        mtime: RecordingEnv::now(),
        state: LibraryItemState {
            time_offset: 42,
            ..Default::default()
        },
        behavior_hints: Default::default(),
    }
}
fn preview() -> MetaItemPreview {
    serde_json::from_value(serde_json::json!({"id":"item","name":"Synthetic","type":"series"}))
        .unwrap()
}
fn notification(ctx: &mut Ctx) {
    ctx.notifications
        .items
        .entry("item".into())
        .or_default()
        .insert(
            "video".into(),
            NotificationItem {
                meta_id: "item".into(),
                video_id: "video".into(),
                video_released: RecordingEnv::now(),
            },
        );
}
type HarnessRuntime = Runtime<RecordingEnv, ActualCtxHarness>;
type HarnessRx = RecordedReceiver<RuntimeEvent<RecordingEnv, ActualCtxHarness>>;
fn recorded(
    model: ActualCtxHarness,
) -> (HarnessRuntime, HarnessRx, Arc<Mutex<Vec<DeclinedMutation>>>) {
    let declined = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&declined);
    let receiver: DeclineReceiver = Arc::new(move |input| capture.lock().unwrap().push(input));
    let scope = EffectScope::recorded(model.token.clone());
    let (runtime, rx) = match Runtime::new_recorded(model, vec![], scope, receiver) {
        Ok(pair) => pair,
        Err(_) => panic!("fixture construction unexpectedly refused"),
    };
    (runtime, rx, declined)
}
fn dispatch(runtime: &HarnessRuntime, action: ActionCtx) {
    runtime.dispatch(RuntimeAction {
        action: Action::Ctx(action),
        field: None,
    });
}
fn events<S: Stream<Item = RuntimeEvent<RecordingEnv, ActualCtxHarness>> + Unpin>(
    rx: &mut S,
) -> Vec<Event> {
    let mut output = vec![];
    while let Some(Some(event)) = rx.next().now_or_never() {
        if let RuntimeEvent::CoreEvent(event) = event {
            output.push(event);
        }
    }
    output
}
fn action(index: usize) -> ActionCtx {
    match index {
        0 => ActionCtx::AddToLibrary(preview()),
        1 => ActionCtx::RemoveFromLibrary("item".into()),
        2 => ActionCtx::RewindLibraryItem("item".into()),
        3 => ActionCtx::ToggleLibraryItemNotifications("item".into(), true),
        _ => ActionCtx::ToggleLibraryItemNotifications("item".into(), false),
    }
}
fn action_event(event: &Event) -> bool {
    matches!(
        event,
        Event::LibraryItemAdded { .. }
            | Event::LibraryItemRemoved { .. }
            | Event::LibraryItemRewinded { .. }
            | Event::LibraryItemNotificationsToggled { .. }
    )
}

#[test]
fn common_transport_t01_real_constructor_order_and_legacy_control() {
    for index in 0..5 {
        reset_env();
        let mut model = ActualCtxHarness::new(None);
        notification(&mut model.ctx);
        let (runtime, mut rx, declined) = recorded(model);
        dispatch(&runtime, action(index));
        let recorded_events = events(&mut rx);
        assert!(declined.lock().unwrap().is_empty());
        assert_eq!(
            recorded_events
                .iter()
                .filter(|event| action_event(event))
                .count(),
            1
        );
        if index == 1 {
            assert!(matches!(
                &recorded_events[..],
                [
                    Event::NotificationsDismissed { .. },
                    Event::LibraryItemRemoved { .. }
                ]
            ));
        } else if index == 3 {
            assert!(matches!(
                &recorded_events[..],
                [
                    Event::LibraryItemNotificationsToggled { .. },
                    Event::NotificationsDismissed { .. }
                ]
            ));
        }
        // A separate genuine Legacy Runtime/Ctx control, not the F2 negative.
        reset_env();
        let mut model = ActualCtxHarness::new(None);
        notification(&mut model.ctx);
        let (runtime, mut rx) = Runtime::new(model, vec![], 128);
        dispatch(&runtime, action(index));
        assert_eq!(recorded_events, events(&mut rx));
    }
}

#[test]
fn common_transport_t02_current_refusal_consumes_only_four_tails() {
    for index in 0..5 {
        reset_env();
        let mut model = ActualCtxHarness::new(Some(AdmissionOutcome::RefusedCurrent(
            AdmissionReason::Unresolved,
        )));
        notification(&mut model.ctx);
        let before = model.ctx.library.clone(); // fixture snapshot, not capture metadata
        let original_scope = EffectScope::recorded(model.token.clone());
        let (expected_item, expected_tail) = action_parts(&model.ctx, index);
        use crate::runtime::LibraryActionKind;
        let expected_kind = match index {
            0 => LibraryActionKind::Added,
            1 => LibraryActionKind::Removed,
            2 => LibraryActionKind::Rewinded,
            _ => LibraryActionKind::NotificationsToggled {
                dismiss: index == 3,
            },
        };
        assert_eq!(expected_tail.kind(), &expected_kind);
        let (runtime, mut rx, declined) = recorded(model);
        dispatch(&runtime, action(index));
        let emitted = events(&mut rx);
        assert_eq!(emitted.len(), 1);
        assert!(
            matches!(&emitted[0], Event::Error { error: CtxError::Admission(AdmissionError { reason: AdmissionReason::Unresolved }), source }
            if source.as_ref() == &expected_tail.source())
        );
        assert_eq!(runtime.model().unwrap().ctx.library, before);
        assert!(runtime
            .model()
            .unwrap()
            .ctx
            .notifications
            .items
            .contains_key("item"));
        CONSTRUCTED.with(|keys| assert!(keys.borrow().is_empty()));
        let mut declined = declined.lock().unwrap();
        assert_eq!(declined.len(), 1);
        let input = declined.pop().unwrap();
        assert_declined_library(
            &input,
            &expected_item,
            &original_scope,
            Some(&expected_tail),
        );
        assert_eq!(
            input.reason,
            DeclineReason::Current(AdmissionReason::Unresolved)
        );
    }
}

#[test]
fn common_transport_t03_actual_frame_desugaring_negative() {
    reset_env();
    let mut model = ActualCtxHarness::new(Some(AdmissionOutcome::RefusedCurrent(
        AdmissionReason::Unresolved,
    )));
    model.desugar = true;
    let before = model.ctx.library.clone();
    let (runtime, mut rx, declined) = recorded(model);
    dispatch(&runtime, ActionCtx::RewindLibraryItem("item".into()));
    assert_eq!(runtime.model().unwrap().ctx.library, before);
    assert!(events(&mut rx)
        .iter()
        .any(|event| matches!(event, Event::LibraryItemRewinded { .. })));
    let declined = declined.lock().unwrap();
    assert_eq!(declined.len(), 1);
    assert!(declined[0].continuation.is_none()); // old bare child cannot govern the flat tail
}

#[test]
fn common_transport_t04_equal_child_accepts_but_refused_equal_does_not() {
    for refused in [false, true] {
        reset_env();
        let mut model = ActualCtxHarness::new(if refused {
            Some(AdmissionOutcome::RefusedCurrent(AdmissionReason::Retired))
        } else {
            None
        });
        model
            .ctx
            .library
            .items
            .get_mut("item")
            .unwrap()
            .state
            .time_offset = 0;
        let (runtime, mut rx, declined) = recorded(model);
        dispatch(&runtime, ActionCtx::RewindLibraryItem("item".into()));
        let emitted = events(&mut rx);
        assert_eq!(
            emitted.iter().filter(|event| action_event(event)).count(),
            usize::from(!refused)
        );
        assert_eq!(declined.lock().unwrap().len(), usize::from(refused));
        CONSTRUCTED.with(|keys| assert!(keys.borrow().is_empty()));
    }
}

#[test]
fn common_transport_t05_notification_removal_survives_timestamp_refusal() {
    reset_env();
    let mut model = ActualCtxHarness::new(Some(AdmissionOutcome::RefusedCurrent(
        AdmissionReason::Unresolved,
    )));
    notification(&mut model.ctx);
    let before = model.ctx.library.clone();
    let (runtime, mut rx, declined) = recorded(model);
    dispatch(&runtime, ActionCtx::DismissNotificationItem("item".into()));
    assert!(!runtime
        .model()
        .unwrap()
        .ctx
        .notifications
        .items
        .contains_key("item"));
    assert_eq!(runtime.model().unwrap().ctx.library, before);
    assert!(matches!(&events(&mut rx)[..], [Event::NotificationsDismissed { id }] if id == "item"));
    assert_eq!(declined.lock().unwrap().len(), 1);
    CONSTRUCTED.with(|keys| assert_eq!(keys.borrow().len(), 1)); // real NotificationsChanged setter
    drain_queue();
    assert!(events(&mut rx)
        .iter()
        .any(|event| matches!(event, Event::NotificationsPushedToStorage { .. })));
}

#[test]
fn common_transport_t06_stale_and_unsupported_outcome_have_no_ui_error() {
    for outcome in [
        AdmissionOutcome::DroppedScope(ScopeDrop::StaleOrigin),
        AdmissionOutcome::ProtocolRejected(ProtocolRejection::Unsupported),
    ] {
        reset_env();
        let (runtime, mut rx, declined) = recorded(ActualCtxHarness::new(Some(outcome)));
        dispatch(&runtime, ActionCtx::RewindLibraryItem("item".into()));
        assert!(events(&mut rx).is_empty());
        assert_eq!(declined.lock().unwrap().len(), 1);
        CONSTRUCTED.with(|keys| assert!(keys.borrow().is_empty()));
    }
}

#[test]
fn common_transport_t07_prefix_and_unrelated_sentinels_survive_refusal() {
    reset_env();
    let mut model = ActualCtxHarness::new(Some(AdmissionOutcome::RefusedCurrent(
        AdmissionReason::Poisoned,
    )));
    model.prefix_and_sentinels = true;
    let (runtime, mut rx, declined) = recorded(model);
    dispatch(&runtime, ActionCtx::RewindLibraryItem("item".into()));
    assert_eq!(declined.lock().unwrap().len(), 1);
    assert_eq!(events(&mut rx).len(), 1); // admission error; futures have not run
    drain_queue();
    EXECUTED.with(|trace| assert_eq!(&*trace.borrow(), &["prefix", "before", "after"]));
    assert_eq!(events(&mut rx).len(), 2); // both unrelated sentinel events retained
}

#[test]
fn common_transport_t08_current_admission_error_is_additive_map_shape() {
    let actual = serde_json::to_value(CtxError::Admission(AdmissionError {
        reason: AdmissionReason::Unresolved,
    }))
    .unwrap();
    assert_eq!(
        actual,
        serde_json::json!({"type":"Admission","reason":"unresolved"})
    );
    let old = serde_json::to_value(CtxError::Other(OtherError::LibraryItemNotFound)).unwrap();
    assert_eq!(
        old,
        serde_json::json!({"type":"Other","code":2,"message":"Item is not found in library"})
    );
}

#[test]
fn common_transport_t09_parent_child_hook_is_after_unlock_before_admission() {
    reset_env();
    let model = ActualCtxHarness::new(None);
    let injected = Arc::clone(&model.outcomes);
    let (runtime, mut rx, declined) = recorded(model);
    let (release_tx, release_rx) = mpsc::channel();
    let (handed_off_tx, handed_off_rx) = mpsc::channel();
    // Worker is paused BEFORE any execute/owner acquisition. This phase only injects
    // the disposition; the actual Owner.execute + Unknown/pending handoff is phase two.
    let worker = thread::spawn(move || {
        release_rx.recv().unwrap();
        injected
            .lock()
            .unwrap()
            .push_back(AdmissionOutcome::RefusedCurrent(
                AdmissionReason::Unresolved,
            ));
        // The injected-state lock has been released before this marker.
        handed_off_tx.send(()).unwrap();
    });
    let handed_off_rx = Mutex::new(handed_off_rx);
    let model = Arc::clone(&runtime.model);
    *runtime.before_library_child.lock().unwrap() = Some(Arc::new(move || {
        assert!(
            model.try_read().is_ok(),
            "parent model write guard is released"
        );
        release_tx.send(()).unwrap();
        handed_off_rx.lock().unwrap().recv().unwrap();
        // NEVER await a FIFO task or Runtime callback while transition is held.
    }));
    dispatch(&runtime, ActionCtx::RewindLibraryItem("item".into()));
    worker.join().unwrap(); // after dispatch released transition
    assert_eq!(declined.lock().unwrap().len(), 1);
    assert!(!events(&mut rx).iter().any(action_event));
}

// Malformed Model output is deliberate transport input, not a second reducer.
#[derive(Clone, Copy)]
enum PlanFault {
    OtherAccepted,
    OtherNoChange,
    OtherDomainDenied,
    OtherRefusedWithSource,
    WrongSubject,
    WrongScope,
    Ungoverned,
    OtherUnsupportedWithSource,
}
impl PlanFault {
    fn reason(self) -> ProtocolRejection {
        match self {
            Self::WrongScope => ProtocolRejection::WrongScope,
            Self::Ungoverned => ProtocolRejection::MissingOutcome,
            Self::OtherUnsupportedWithSource => ProtocolRejection::Unsupported,
            _ => ProtocolRejection::WrongSubject,
        }
    }
}
fn action_frame(ctx: &Ctx, index: usize) -> LibraryUpdateFrame {
    match plan_library_action::<RecordingEnv>(
        &ctx.library,
        &Msg::Action(Action::Ctx(action(index))),
    )
    .unwrap()
    {
        LibraryActionPlan::Update(frame) => frame,
        LibraryActionPlan::Denied(_) => {
            panic!("expected the real action constructor to produce a frame")
        }
    }
}
fn action_parts(ctx: &Ctx, index: usize) -> (LibraryItem, LibraryContinuation) {
    action_frame(ctx, index).into_parts()
}
fn notification_child(ctx: &Ctx) -> LibraryItem {
    // Follow the actual Action -> Internal dismissal on a snapshot; do not execute its outputs.
    let mut snapshot = ctx.clone();
    let dismiss = Update::<RecordingEnv>::update(&mut snapshot, &Msg::Action(Action::Ctx(ActionCtx::DismissNotificationItem("item".into()))))
        .into_iter().find_map(|effect| match effect {
            Effect::Msg(msg) if matches!(&*msg, Msg::Internal(Internal::DismissNotificationItem(id)) if id == "item") => Some(*msg),
            _ => None,
        }).expect("actual action must forward notification dismissal");
    Update::<RecordingEnv>::update(&mut snapshot, &dismiss)
        .into_iter()
        .find_map(|effect| match effect {
            Effect::Msg(msg) => match *msg {
                Msg::Internal(Internal::UpdateLibraryItem(item)) => Some(item),
                _ => None,
            },
            _ => None,
        })
        .expect("actual notification reducer must plan its timestamp child")
}
fn faulty_plan(ctx: &Ctx, scope: &EffectScope, fault: PlanFault) -> ScopedPlan<Field> {
    let nested = action_frame(ctx, 2);
    let mut direct = DirectOutcome::library(scope, "item".into(), AdmissionOutcome::AcceptedChange);
    direct.kind = match fault {
        PlanFault::OtherAccepted => DirectKind::Other {
            outcome: AdmissionOutcome::AcceptedChange,
            error_source: None,
        },
        PlanFault::OtherNoChange => DirectKind::Other {
            outcome: AdmissionOutcome::AcceptedNoChange,
            error_source: None,
        },
        PlanFault::OtherDomainDenied => DirectKind::Other {
            outcome: AdmissionOutcome::DomainDenied,
            error_source: None,
        },
        PlanFault::OtherRefusedWithSource => DirectKind::Other {
            outcome: AdmissionOutcome::RefusedCurrent(AdmissionReason::Unresolved),
            error_source: Some(Event::LibraryItemAdded {
                id: "fabricated-source".into(),
            }),
        },
        PlanFault::OtherUnsupportedWithSource => DirectKind::Other {
            outcome: AdmissionOutcome::ProtocolRejected(ProtocolRejection::Unsupported),
            error_source: Some(Event::LibraryItemAdded {
                id: "fabricated-source".into(),
            }),
        },
        PlanFault::WrongSubject => DirectKind::LibraryItem {
            id: "wrong-item".into(),
            outcome: AdmissionOutcome::AcceptedChange,
        },
        PlanFault::WrongScope => {
            direct.scope = EffectScope::recorded(ScopeToken::new(()));
            DirectKind::LibraryItem {
                id: "item".into(),
                outcome: AdmissionOutcome::AcceptedChange,
            }
        }
        PlanFault::Ungoverned => DirectKind::Ungoverned,
    };
    ScopedPlan {
        fields: vec![Field::Ctx],
        effects: vec![
            ScopedEffect::ordinary(
                Effect::Msg(Box::new(Msg::Event(Event::NotificationsDismissed {
                    id: "malformed-event".into(),
                }))),
                scope.descendant(),
            ),
            sentinel("malformed-sentinel", scope),
            ScopedEffect::library(nested, scope.descendant()),
        ],
        // Fixture premise: this is separately admitted work, even though the returned
        // pairing metadata is corrupted. It records transfer, NOT Owner publication.
        prefix: Some(LocalPrefix {
            origin: scope.origin().expect("recorded fixture").clone(),
            future: async {
                EXECUTED.with(|trace| trace.borrow_mut().push("admitted-prefix".into()));
                PrefixCompletion::Final([None, None])
            }
            .boxed_env(),
        }),
        direct,
    }
}
fn assert_declined_library(
    input: &DeclinedMutation,
    expected: &LibraryItem,
    scope: &EffectScope,
    tail: Option<&LibraryContinuation>,
) {
    assert!(
        matches!(&input.input, Msg::Internal(Internal::UpdateLibraryItem(item)) if item == expected)
    );
    assert!(input.scope.same_scope(scope));
    match (&input.continuation, tail) {
        (Some(actual), Some(expected)) => {
            assert_eq!(actual.id(), expected.id());
            assert_eq!(actual.kind(), expected.kind());
            assert_eq!(actual.source(), expected.source());
        }
        (None, None) => {}
        _ => panic!("original continuation ownership mismatch"),
    }
}
fn observations(rx: &mut HarnessRx) -> (usize, Vec<Event>) {
    let mut fields = 0;
    let mut events = vec![];
    while let Some(Some(event)) = rx.next().now_or_never() {
        match event {
            RuntimeEvent::NewState(..) => fields += 1,
            RuntimeEvent::CoreEvent(event) => events.push(event),
        }
    }
    (fields, events)
}

#[test]
fn common_transport_t10_pairing_precedes_child_output_on_both_paths() {
    for framed in [false, true] {
        for fault in [
            PlanFault::OtherAccepted,
            PlanFault::OtherNoChange,
            PlanFault::OtherDomainDenied,
            PlanFault::OtherRefusedWithSource,
            PlanFault::WrongSubject,
            PlanFault::WrongScope,
            PlanFault::Ungoverned,
            PlanFault::OtherUnsupportedWithSource,
        ] {
            reset_env();
            let mut model = ActualCtxHarness::new(None);
            notification(&mut model.ctx);
            let original_scope = EffectScope::recorded(model.token.clone());
            let before = model.ctx.library.clone();
            let (expected, tail) = if framed {
                let (item, tail) = action_parts(&model.ctx, 2);
                (item, Some(tail))
            } else {
                (notification_child(&model.ctx), None)
            };
            model.fault = Some(fault);
            model.prefix_and_sentinels = true;
            let (runtime, mut rx, declined) = recorded(model);
            dispatch(
                &runtime,
                if framed {
                    action(2)
                } else {
                    ActionCtx::DismissNotificationItem("item".into())
                },
            );
            let (fields, immediate) = observations(&mut rx);
            assert_eq!(fields, usize::from(!framed)); // only the actual parent's notification removal
            if framed {
                assert!(immediate.is_empty());
            } else {
                assert_eq!(
                    immediate,
                    vec![Event::NotificationsDismissed { id: "item".into() }]
                );
            }
            assert_eq!(runtime.model().unwrap().ctx.library, before);
            assert_eq!(
                runtime
                    .model()
                    .unwrap()
                    .ctx
                    .notifications
                    .items
                    .contains_key("item"),
                framed
            );
            {
                let declined = declined.lock().unwrap();
                assert_eq!(declined.len(), 1);
                assert_declined_library(&declined[0], &expected, &original_scope, tail.as_ref());
                assert_eq!(declined[0].reason, DeclineReason::Protocol(fault.reason()));
            }
            CONSTRUCTED.with(|keys| assert_eq!(keys.borrow().len(), usize::from(!framed)));
            PLANNED_CHILDREN.with(|count| assert_eq!(count.get(), 1)); // nested frame was never entered
            drain_queue();
            let (fields, later) = observations(&mut rx);
            assert_eq!(fields, 0);
            assert!(!later
                .iter()
                .any(|event| action_event(event) || matches!(event, Event::Error { .. })));
            assert!(later.iter().all(|event| !matches!(event, Event::NotificationsDismissed { id } if id == "malformed-event")));
            assert_eq!(later.iter().filter(|event| matches!(event, Event::NotificationsDismissed { id } if id.starts_with("sentinel-"))).count(), 2);
            if !framed {
                assert!(later
                    .iter()
                    .any(|event| matches!(event, Event::NotificationsPushedToStorage { .. })));
            }
            EXECUTED.with(|trace| {
                let trace = trace.borrow();
                for name in ["prefix", "before", "admitted-prefix", "after"] {
                    assert_eq!(
                        trace.iter().filter(|entry| entry.as_str() == name).count(),
                        1
                    );
                }
                let at = |name| trace.iter().position(|entry| entry == name).unwrap();
                assert!(
                    at("prefix") < at("before")
                        && at("before") < at("admitted-prefix")
                        && at("admitted-prefix") < at("after")
                );
                assert!(!trace.iter().any(|entry| entry == "malformed-sentinel"));
            });
            PLANNED_CHILDREN.with(|count| assert_eq!(count.get(), 1));
        }
    }
}

#[test]
fn common_transport_t11_root_wrong_scope_blocks_ordinary_output_not_prefix() {
    reset_env();
    let mut model = ActualCtxHarness::new(None);
    let original_scope = EffectScope::recorded(model.token.clone());
    let before = model.ctx.library.clone();
    model.fault = Some(PlanFault::WrongScope);
    model.fault_at_root = true;
    let (runtime, mut rx, declined) = recorded(model);
    dispatch(&runtime, action(2));
    let (fields, immediate) = observations(&mut rx);
    assert_eq!(fields, 0);
    assert!(immediate.is_empty());
    assert_eq!(runtime.model().unwrap().ctx.library, before);
    {
        let declined = declined.lock().unwrap();
        assert_eq!(declined.len(), 1);
        assert!(
            matches!(&declined[0].input, Msg::Action(Action::Ctx(ActionCtx::RewindLibraryItem(id))) if id == "item")
        );
        assert!(declined[0].scope.same_scope(&original_scope));
        assert!(declined[0].continuation.is_none());
        assert_eq!(
            declined[0].reason,
            DeclineReason::Protocol(ProtocolRejection::WrongScope)
        );
    }
    drain_queue();
    assert_eq!(observations(&mut rx), (0, vec![]));
    EXECUTED.with(|trace| assert_eq!(&*trace.borrow(), &["admitted-prefix"]));
    PLANNED_CHILDREN.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn common_transport_t12_valid_domain_results_remain_distinct() {
    reset_env();
    let mut model = ActualCtxHarness::new(None);
    model.ctx.library.items.clear();
    let (runtime, mut rx, declined) = recorded(model);
    dispatch(&runtime, action(1));
    assert_eq!(
        events(&mut rx),
        vec![Event::Error {
            error: CtxError::Other(OtherError::LibraryItemNotFound),
            source: Box::new(Event::LibraryItemRemoved { id: "item".into() }),
        }]
    );
    assert!(declined.lock().unwrap().is_empty());
    // Correctly paired LibraryItem/DomainDenied is not Other/DomainDenied:
    // no success continuation or fabricated admission/protocol error.
    reset_env();
    let (runtime, mut rx, declined) =
        recorded(ActualCtxHarness::new(Some(AdmissionOutcome::DomainDenied)));
    dispatch(&runtime, action(2));
    assert!(events(&mut rx).is_empty());
    assert!(declined.lock().unwrap().is_empty());
    CONSTRUCTED.with(|keys| assert!(keys.borrow().is_empty()));
}

#[derive(Clone)]
struct LegacyDefaults(ActualCtxHarness);
impl Model<RecordingEnv> for LegacyDefaults {
    type Field = Field;
    fn update(&mut self, msg: &Msg) -> (Vec<Effect>, Vec<Field>) {
        self.0.update(msg)
    }
    fn update_field(&mut self, msg: &Msg, field: &Field) -> (Vec<Effect>, Vec<Field>) {
        self.0.update_field(msg, field)
    }
    // All scoped/capability methods deliberately use the actual Model defaults.
}

#[test]
fn common_transport_t13_defaults_bootstrap_and_descendant_refusals() {
    reset_env();
    let legacy = LegacyDefaults(ActualCtxHarness::new(None));
    let (runtime, mut rx) = Runtime::<RecordingEnv, LegacyDefaults>::new(legacy, vec![], 128);
    runtime.dispatch(RuntimeAction {
        action: Action::Ctx(action(2)),
        field: None,
    });
    assert_eq!(
        runtime.model().unwrap().0.ctx.library.items["item"]
            .state
            .time_offset,
        0
    );
    let mut got_legacy_event = false;
    while let Some(Some(event)) = rx.next().now_or_never() {
        if let RuntimeEvent::CoreEvent(Event::LibraryItemRewinded { id }) = event {
            assert_eq!(id, "item");
            got_legacy_event = true;
        }
    }
    assert!(got_legacy_event);
    reset_env();
    let mut legacy = LegacyDefaults(ActualCtxHarness::new(None));
    let scope = EffectScope::recorded(legacy.0.token.clone());
    let before = legacy.0.ctx.library.clone();
    let rejected = legacy.update_scoped(&Msg::Action(Action::Ctx(action(2))), &scope);
    assert!(matches!(
        rejected.direct.kind,
        DirectKind::Other {
            outcome: AdmissionOutcome::ProtocolRejected(ProtocolRejection::Unsupported),
            ..
        }
    ));
    assert!(rejected.effects.is_empty() && rejected.fields.is_empty() && rejected.prefix.is_none());
    assert_eq!(legacy.0.ctx.library, before);
    let declines: DeclineReceiver =
        Arc::new(|_| panic!("constructor must return owned inputs, not invoke decline"));
    match Runtime::<RecordingEnv, LegacyDefaults>::new_recorded(
        legacy,
        vec![sentinel("must-not-run", &scope)],
        scope.clone(),
        declines.clone(),
    ) {
        Err((model, effects, returned_scope)) => {
            assert_eq!(model.0.ctx.library, before);
            assert_eq!(effects.len(), 1);
            assert!(returned_scope.same_scope(&scope));
        }
        Ok(_) => panic!("default unsupported capability was accepted"),
    }
    let model = ActualCtxHarness::new(None);
    let before = model.ctx.library.clone();
    let missing = EffectScope::missing();
    match Runtime::new_recorded(
        model,
        vec![sentinel("must-not-run", &missing)],
        missing.clone(),
        declines,
    ) {
        Err((model, effects, returned_scope)) => {
            assert_eq!(model.ctx.library, before);
            assert_eq!(effects.len(), 1);
            assert!(returned_scope.same_scope(&missing));
        }
        Ok(_) => panic!("missing bootstrap origin was accepted"),
    }
    QUEUE.with(|queue| assert!(queue.borrow().is_empty()));
    for scope in [EffectScope::missing(), EffectScope::Legacy] {
        reset_env();
        let mut model = ActualCtxHarness::new(None);
        let before = model.ctx.library.clone();
        let (expected, tail) = action_parts(&model.ctx, 2);
        model.child_scope = Some(scope.clone());
        let (runtime, mut rx, declined) = recorded(model);
        dispatch(&runtime, action(2));
        assert!(events(&mut rx).is_empty());
        assert_eq!(runtime.model().unwrap().ctx.library, before);
        let declined = declined.lock().unwrap();
        assert_eq!(declined.len(), 1);
        assert_declined_library(&declined[0], &expected, &scope, Some(&tail));
        let reason = if matches!(scope, EffectScope::Legacy) {
            DeclineReason::Protocol(ProtocolRejection::WrongScope)
        } else {
            DeclineReason::Scope(ScopeDrop::Missing)
        };
        assert_eq!(declined[0].reason, reason);
        PLANNED_CHILDREN.with(|count| assert_eq!(count.get(), 0)); // no fallback to current root
        CONSTRUCTED.with(|keys| assert!(keys.borrow().is_empty()));
    }
}
