use crate::runtime::msg::{Action, ActionCtx, Event, Internal, Msg};
use crate::runtime::{
    admission_error, AdmissionOutcome, DeclineReason, DeclineReceiver, DeclinedMutation,
    DirectKind, DirectOutcome, Effect, EffectFuture, EffectScope, Env, LibraryContinuation,
    LibraryUpdateFrame, Model, ProtocolRejection, ScopeDrop, ScopedEffect, ScopedEffectKind,
    ScopedPlan,
};
use derivative::Derivative;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::FutureExt;
#[cfg(test)]
use futures::SinkExt;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LockResult, Mutex, RwLock, RwLockReadGuard};

#[derive(Serialize, Debug, PartialEq, Eq)]
#[serde(tag = "name", content = "args")]
pub enum RuntimeEvent<E: Env, M: Model<E>> {
    NewState(Vec<M::Field>, #[cfg(test)] M),
    CoreEvent(Event),
}

#[derive(Debug)]
pub struct RuntimeAction<E: Env, M: Model<E>> {
    pub field: Option<M::Field>,
    pub action: Action,
}

#[derive(Clone, Copy)]
struct AddonEffectContext {
    epoch: u64,
    fenced: bool,
}

struct PlannedEffects<F> {
    plan: ScopedPlan<F>,
    addon_context: AddonEffectContext,
}

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Runtime<E: Env, M: Model<E>> {
    model: Arc<RwLock<M>>,
    tx: Sender<RuntimeEvent<E, M>>,
    addon_epoch: Arc<AtomicU64>,
    transition: Arc<Mutex<()>>,
    declines: Option<DeclineReceiver>,
    #[cfg(test)]
    before_library_child: Arc<Mutex<Option<Arc<dyn Fn() + Send + Sync>>>>,
    env: PhantomData<E>,
}

impl<E, M> Runtime<E, M>
where
    E: Env + Send + 'static,
    M: Model<E> + Send + Sync + 'static,
{
    /// Legacy construction and mutation entrypoints remain exactly opt-out.
    pub fn new(
        model: M,
        effects: Vec<Effect>,
        buffer: usize,
    ) -> (Self, Receiver<RuntimeEvent<E, M>>) {
        let (runtime, rx) = Self::allocate(model, buffer, None);
        {
            let _transition = runtime.transition.lock().expect("transition lock failed");
            let plan = ScopedPlan::ordinary(effects, vec![], &EffectScope::Legacy);
            runtime.handle_effects(PlannedEffects {
                plan,
                addon_context: AddonEffectContext {
                    epoch: 0,
                    fenced: false,
                },
            });
        }
        (runtime, rx)
    }

    /// Explicit source-unit opt-in. Failed construction returns all owned inputs intact.
    /// This does not validate storage, hydrate an owner or activate a native backend.
    pub fn new_recorded(
        model: M,
        effects: Vec<ScopedEffect>,
        scope: EffectScope,
        buffer: usize,
        declines: DeclineReceiver,
    ) -> Result<(Self, Receiver<RuntimeEvent<E, M>>), (M, Vec<ScopedEffect>, EffectScope)> {
        if !model.supports_recorded_dispatch() || scope.origin().is_none() {
            return Err((model, effects, scope));
        }
        let (runtime, rx) = Self::allocate(model, buffer, Some(declines));
        {
            let _transition = runtime.transition.lock().expect("transition lock failed");
            let mut plan = ScopedPlan::empty(&scope);
            plan.effects = effects;
            runtime.handle_effects(PlannedEffects {
                plan,
                addon_context: AddonEffectContext {
                    epoch: 0,
                    fenced: false,
                },
            });
        }
        Ok((runtime, rx))
    }

    fn allocate(
        model: M,
        buffer: usize,
        declines: Option<DeclineReceiver>,
    ) -> (Self, Receiver<RuntimeEvent<E, M>>) {
        let (tx, rx) = channel(buffer);
        (
            Self {
                model: Arc::new(RwLock::new(model)),
                tx,
                addon_epoch: Arc::new(AtomicU64::new(0)),
                transition: Arc::new(Mutex::new(())),
                declines,
                env: PhantomData,
                #[cfg(test)]
                before_library_child: Arc::new(Mutex::new(None)),
            },
            rx,
        )
    }

    pub fn model(&self) -> LockResult<RwLockReadGuard<'_, M>> {
        self.model.read()
    }

    pub fn dispatch(&self, action: RuntimeAction<E, M>) {
        let _transition = self.transition.lock().expect("transition lock failed");
        let RuntimeAction { action, field } = action;
        let msg = Msg::Action(action);
        let (planned, scope) = self.plan_action_scoped(&msg, field.as_ref());
        let addon_context = planned.addon_context;
        let direct = self.handle_invocation(planned, &msg, &scope);
        self.finish_bare(msg, scope, direct, addon_context);
    }

    fn plan_action_scoped(
        &self,
        msg: &Msg,
        field: Option<&M::Field>,
    ) -> (PlannedEffects<M::Field>, EffectScope) {
        let invalidates = field.is_none()
            && matches!(msg, Msg::Action(action) if is_local_addon_mutation(action));
        let mut model = self.model.write().expect("model write failed");
        let scope = if self.declines.is_some() {
            model
                .root_scope()
                .map(EffectScope::recorded)
                .unwrap_or_else(EffectScope::missing)
        } else {
            EffectScope::Legacy
        };
        let plan = if self.declines.is_some() {
            if scope.origin().is_none() {
                reject_for(
                    msg,
                    &scope,
                    AdmissionOutcome::DroppedScope(ScopeDrop::Missing),
                )
            } else {
                match field {
                    Some(field) => model.update_field_scoped(msg, field, &scope),
                    None => model.update_scoped(msg, &scope),
                }
            }
        } else {
            let (effects, fields) = match field {
                Some(field) => model.update_field(msg, field),
                None => model.update(msg),
            };
            ScopedPlan::ordinary(effects, fields, &scope)
        };
        // Preserve the existing addon invalidation-on-local-root-attempt rule.
        if invalidates {
            self.addon_epoch.fetch_add(1, Ordering::SeqCst);
        }
        let addon_context = AddonEffectContext {
            epoch: self.addon_epoch.load(Ordering::SeqCst),
            fenced: false,
        };
        (
            PlannedEffects {
                plan,
                addon_context,
            },
            scope,
        )
    }

    #[cfg(test)]
    pub async fn close(&mut self) -> Result<(), anyhow::Error> {
        self.tx.flush().await?;
        self.tx.close_channel();
        Ok(())
    }

    fn emit(&self, event: RuntimeEvent<E, M>) {
        self.tx.clone().try_send(event).expect("emit event failed");
    }

    /// Bootstrap and preserved Legacy test entrypoint: no returned Model invocation.
    fn handle_effects(&self, planned: PlannedEffects<M::Field>) -> DirectOutcome {
        self.deliver_plan(planned, None)
    }

    /// Every actual Model invocation pairs before its ordinary output is authorized.
    fn handle_invocation(
        &self,
        planned: PlannedEffects<M::Field>,
        msg: &Msg,
        scope: &EffectScope,
    ) -> DirectOutcome {
        self.deliver_plan(planned, Some((msg, scope)))
    }

    fn deliver_plan(
        &self,
        planned: PlannedEffects<M::Field>,
        invocation: Option<(&Msg, &EffectScope)>,
    ) -> DirectOutcome {
        let PlannedEffects {
            plan,
            addon_context,
        } = planned;
        let ScopedPlan {
            effects,
            fields,
            prefix,
            direct,
        } = plan;
        // This prefix is separately asserted already-admitted by the Model contract.
        // Pairing/UI rejection must not silently erase its pending durable work.
        if let Some(prefix) = prefix {
            let runtime = self.clone();
            E::exec_sequential(prefix.future.then(move |events| async move {
                // Backend/owner/completion-holder guards must already have been released.
                runtime.handle_completion(events, addon_context);
            }));
        }
        let (direct, ordinary_allowed) = match invocation {
            Some((msg, scope)) if self.declines.is_some() => pair_direct(msg, scope, direct),
            _ => (direct, true), // Legacy output is not a recorded admission claim.
        };
        // Drop only this invocation's still-owned ordinary output. Parent/siblings
        // are on the surrounding Runtime stack and remain independently traversable.
        if !ordinary_allowed || self.addon_stale(addon_context) {
            return direct;
        }
        if !fields.is_empty() {
            if addon_context.fenced {
                let model = self.model.read().expect("model read failed");
                if self.addon_stale(addon_context) {
                    return direct;
                }
                #[cfg(test)]
                let snapshot = model.to_owned();
                self.emit(RuntimeEvent::<E, M>::NewState(
                    fields,
                    #[cfg(test)]
                    snapshot,
                ));
                drop(model);
            } else {
                #[cfg(test)]
                let model = self.model.read().expect("model read failed");
                self.emit(RuntimeEvent::<E, M>::NewState(
                    fields,
                    #[cfg(test)]
                    model.to_owned(),
                ));
            }
        }
        for effect in effects {
            self.handle_one(effect, addon_context);
        }
        direct
    }

    fn handle_one(&self, effect: ScopedEffect, addon_context: AddonEffectContext) {
        let ScopedEffect { effect, scope } = effect;
        match effect {
            ScopedEffectKind::LibraryUpdateThen(frame) => {
                self.handle_library_frame(frame, scope, addon_context)
            }
            ScopedEffectKind::Ordinary(Effect::Msg(msg)) => {
                self.handle_effect_output_scoped(*msg, scope, addon_context)
            }
            ScopedEffectKind::Ordinary(Effect::Future(EffectFuture::Sequential(future))) => {
                let runtime = self.clone();
                E::exec_sequential(future.then(move |msg| async move {
                    runtime.handle_scheduled_effect_output(msg, scope, addon_context);
                }));
            }
            ScopedEffectKind::Ordinary(Effect::Future(EffectFuture::Concurrent(future))) => {
                let runtime = self.clone();
                E::exec_concurrent(future.then(move |msg| async move {
                    runtime.handle_scheduled_effect_output(msg, scope, addon_context);
                }));
            }
        }
    }

    fn handle_scheduled_effect_output(
        &self,
        msg: Msg,
        scope: EffectScope,
        addon_context: AddonEffectContext,
    ) {
        let _transition = self.transition.lock().expect("transition lock failed");
        self.handle_effect_output_scoped(msg, scope, addon_context);
    }

    fn handle_completion(&self, events: [Option<Event>; 2], addon_context: AddonEffectContext) {
        let _transition = self.transition.lock().expect("transition lock failed");
        // Event-only: never plan_internal, update Ctx/screens, refresh Origin or retry a write.
        for event in events.into_iter().flatten() {
            self.emit_core(event, addon_context);
        }
    }

    fn addon_stale(&self, context: AddonEffectContext) -> bool {
        context.fenced && context.epoch != self.addon_epoch.load(Ordering::SeqCst)
    }

    fn emit_core(&self, event: Event, addon_context: AddonEffectContext) {
        if addon_context.fenced {
            let model = self.model.read().expect("model read failed");
            if self.addon_stale(addon_context) {
                return;
            }
            self.emit(RuntimeEvent::CoreEvent(event));
            drop(model);
        } else {
            self.emit(RuntimeEvent::CoreEvent(event));
        }
    }

    fn handle_effect_output_scoped(
        &self,
        msg: Msg,
        scope: EffectScope,
        addon_context: AddonEffectContext,
    ) {
        match msg {
            Msg::Event(event) => self.emit_core(event, addon_context),
            Msg::Internal(_) => {
                if matches!(&msg, Msg::Internal(Internal::UpdateLibraryItem(_))) {
                    self.run_before_library_child();
                }
                let planned = self.plan_internal_scoped(&msg, &scope, addon_context);
                let context = planned.addon_context;
                let direct = self.handle_invocation(planned, &msg, &scope);
                self.finish_bare(msg, scope, direct, context);
            }
            Msg::Action(_) => panic!("effects are not allowed to resolve with action"),
        }
    }

    fn plan_internal_scoped(
        &self,
        msg: &Msg,
        scope: &EffectScope,
        addon_context: AddonEffectContext,
    ) -> PlannedEffects<M::Field> {
        let context = AddonEffectContext {
            fenced: addon_context.fenced || is_addon_result(msg),
            ..addon_context
        };
        let mut model = self.model.write().expect("model write failed");
        let plan = if self.addon_stale(context) {
            if self.declines.is_some() {
                reject_for(
                    msg,
                    scope,
                    AdmissionOutcome::DroppedScope(ScopeDrop::AddonEpoch),
                )
            } else {
                ScopedPlan::empty(scope)
            }
        } else if self.declines.is_some() {
            if matches!(scope, EffectScope::Legacy) {
                reject_for(
                    msg,
                    scope,
                    AdmissionOutcome::ProtocolRejected(ProtocolRejection::WrongScope),
                )
            } else if scope.origin().is_none() {
                reject_for(
                    msg,
                    scope,
                    AdmissionOutcome::DroppedScope(ScopeDrop::Missing),
                )
            } else {
                model.update_scoped(msg, scope)
            }
        } else {
            let (effects, fields) = model.update(msg);
            ScopedPlan::ordinary(effects, fields, scope)
        };
        let context = if context.fenced {
            context
        } else {
            AddonEffectContext {
                epoch: self.addon_epoch.load(Ordering::SeqCst),
                fenced: false,
            }
        };
        PlannedEffects {
            plan,
            addon_context: context,
        }
    }

    fn handle_library_frame(
        &self,
        frame: LibraryUpdateFrame,
        scope: EffectScope,
        addon_context: AddonEffectContext,
    ) {
        let (item, continuation) = frame.into_parts();
        let valid = item.id == continuation.id();
        let msg = Msg::Internal(Internal::UpdateLibraryItem(item));
        if !valid {
            self.decline(
                msg,
                scope,
                DeclineReason::Protocol(ProtocolRejection::InvalidFrame),
                Some(continuation),
            );
            return;
        }
        self.run_before_library_child();
        let planned = self.plan_internal_scoped(&msg, &scope, addon_context);
        let context = planned.addon_context;
        let direct = self.handle_invocation(planned, &msg, &scope);
        // pair_direct normalizes every rejected library invocation to its original
        // LibraryItem subject; no second, weaker frame-specific pairing rule.
        let outcome = match direct.kind {
            DirectKind::LibraryItem { outcome, .. } => outcome,
            _ => unreachable!("library invocation must be normalized by pair_direct"),
        };
        match outcome {
            AdmissionOutcome::AcceptedChange | AdmissionOutcome::AcceptedNoChange => {
                for msg in continuation.accepted() {
                    self.handle_effect_output_scoped(msg, scope.descendant(), context);
                }
            }
            AdmissionOutcome::RefusedCurrent(reason) => {
                let error = admission_error(continuation.source(), reason);
                self.decline(
                    msg,
                    scope,
                    DeclineReason::Current(reason),
                    Some(continuation),
                );
                self.emit_core(error, context);
            }
            AdmissionOutcome::DroppedScope(reason) => {
                self.decline(msg, scope, DeclineReason::Scope(reason), Some(continuation))
            }
            AdmissionOutcome::ProtocolRejected(reason) => self.decline(
                msg,
                scope,
                DeclineReason::Protocol(reason),
                Some(continuation),
            ),
            AdmissionOutcome::DomainDenied => {} // paired domain outcome: no success continuation
        }
    }

    fn finish_bare(
        &self,
        msg: Msg,
        scope: EffectScope,
        direct: DirectOutcome,
        addon_context: AddonEffectContext,
    ) {
        if self.declines.is_none() {
            return;
        } // Legacy never produces recorded dispositions.
          // Only called with handle_invocation's paired/normalized direct outcome.
        let (outcome, source) = match direct.kind {
            DirectKind::Ungoverned => return, // valid only for non-library inputs
            DirectKind::LibraryItem { outcome, .. } => (outcome, None),
            DirectKind::Other {
                outcome,
                error_source,
            } => (outcome, error_source),
        };
        match outcome {
            AdmissionOutcome::RefusedCurrent(reason) => {
                self.decline(msg, scope, DeclineReason::Current(reason), None);
                if let Some(source) = source {
                    self.emit_core(admission_error(source, reason), addon_context);
                }
            }
            AdmissionOutcome::DroppedScope(reason) => {
                self.decline(msg, scope, DeclineReason::Scope(reason), None)
            }
            AdmissionOutcome::ProtocolRejected(reason) => {
                self.decline(msg, scope, DeclineReason::Protocol(reason), None)
            }
            AdmissionOutcome::AcceptedChange
            | AdmissionOutcome::AcceptedNoChange
            | AdmissionOutcome::DomainDenied => {}
        }
    }

    fn decline(
        &self,
        input: Msg,
        scope: EffectScope,
        reason: DeclineReason,
        continuation: Option<LibraryContinuation>,
    ) {
        // A LibraryUpdateThen can arise only in new_recorded's scoped plan type.
        // No model/owner guard is held. The required receiver must not reenter/await.
        let receiver = self
            .declines
            .as_ref()
            .expect("recorded effect requires decline receiver");
        receiver(DeclinedMutation {
            input,
            scope,
            reason,
            continuation,
        });
    }

    fn run_before_library_child(&self) {
        // Test-only rendezvous after parent planning/model unlock, before child planning.
        // The hook holder mutex is dropped before the hook runs. No production callback.
        #[cfg(test)]
        {
            let hook = self
                .before_library_child
                .lock()
                .expect("hook lock failed")
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
    }

    // Keep existing addon-fence unit tests byte-identical while production uses the
    // explicitly scoped entrypoints. These adapters are Legacy-only test call sites.
    #[cfg(test)]
    fn plan_action(&self, action: RuntimeAction<E, M>) -> PlannedEffects<M::Field> {
        assert!(self.declines.is_none());
        self.plan_action_scoped(&Msg::Action(action.action), action.field.as_ref())
            .0
    }
    #[cfg(test)]
    fn plan_internal(
        &self,
        msg: &Msg,
        context: AddonEffectContext,
    ) -> Option<PlannedEffects<M::Field>> {
        assert!(self.declines.is_none());
        let planned = self.plan_internal_scoped(msg, &EffectScope::Legacy, context);
        if self.addon_stale(planned.addon_context) {
            None
        } else {
            Some(planned)
        }
    }
    #[cfg(test)]
    fn handle_effect_output(&self, msg: Msg, context: AddonEffectContext) {
        assert!(self.declines.is_none());
        self.handle_effect_output_scoped(msg, EffectScope::Legacy, context);
    }
}

/// One pairing rule for roots, bare internals and framed library children.
fn pair_direct(msg: &Msg, scope: &EffectScope, direct: DirectOutcome) -> (DirectOutcome, bool) {
    let reject = |reason| {
        (
            direct_for(msg, scope, AdmissionOutcome::ProtocolRejected(reason)),
            false,
        )
    };
    if !direct.scope.same_scope(scope) {
        return reject(ProtocolRejection::WrongScope);
    }
    match (msg, &direct.kind) {
        (Msg::Internal(Internal::UpdateLibraryItem(item)), DirectKind::LibraryItem { id, .. }) => {
            if &item.id != id {
                return reject(ProtocolRejection::WrongSubject);
            }
        }
        (
            Msg::Internal(Internal::UpdateLibraryItem(_)),
            DirectKind::Other {
                outcome: AdmissionOutcome::ProtocolRejected(reason),
                ..
            },
        ) => return reject(*reason), // explicit rejection only; supplied source discarded
        (Msg::Internal(Internal::UpdateLibraryItem(_)), DirectKind::Other { .. }) => {
            return reject(ProtocolRejection::WrongSubject)
        }
        (Msg::Internal(Internal::UpdateLibraryItem(_)), DirectKind::Ungoverned) => {
            return reject(ProtocolRejection::MissingOutcome)
        }
        (_, DirectKind::LibraryItem { .. }) => return reject(ProtocolRejection::WrongSubject),
        _ => {}
    }
    let ordinary_allowed = match &direct.kind {
        DirectKind::Ungoverned => true, // never library input after the checks above
        DirectKind::LibraryItem { outcome, .. } | DirectKind::Other { outcome, .. } => matches!(
            outcome,
            AdmissionOutcome::AcceptedChange
                | AdmissionOutcome::AcceptedNoChange
                | AdmissionOutcome::DomainDenied
        ),
    };
    (direct, ordinary_allowed)
}

fn direct_for(msg: &Msg, scope: &EffectScope, outcome: AdmissionOutcome) -> DirectOutcome {
    match msg {
        Msg::Internal(Internal::UpdateLibraryItem(item)) => {
            DirectOutcome::library(scope, item.id.clone(), outcome)
        }
        _ => DirectOutcome::rejected(scope, outcome),
    }
}

fn reject_for<F>(msg: &Msg, scope: &EffectScope, outcome: AdmissionOutcome) -> ScopedPlan<F> {
    let mut plan = ScopedPlan::empty(scope);
    plan.direct = direct_for(msg, scope, outcome);
    plan
}

fn is_local_addon_mutation(action: &Action) -> bool {
    matches!(
        action,
        Action::Ctx(
            ActionCtx::InstallAddonLocal(_)
                | ActionCtx::UpgradeAddonLocal(_)
                | ActionCtx::UninstallAddonLocal(_)
                | ActionCtx::ReplaceAddonLocal { .. }
                | ActionCtx::ReplaceAddonProjectionLocal(_)
        )
    )
}

fn is_addon_result(msg: &Msg) -> bool {
    matches!(
        msg,
        Msg::Internal(
            Internal::CtxAuthResult(_, _)
                | Internal::AddonsAPIResult(_, _)
                | Internal::ResourceRequestResult(_, _)
                | Internal::ManifestRequestResult(_, _)
                | Internal::NotificationsRequestResult(_, _)
        )
    )
}

#[cfg(test)]
#[path = "scoped_dispatch_tests.rs"]
mod scoped_dispatch_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ctx::{CtxError, OtherError};
    use crate::types::addon::{ResourcePath, ResourceRequest, ResourceResponse};
    use crate::types::api::AuthRequest;
    use crate::unit_tests::TestEnv;
    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use std::sync::{mpsc, Arc, Barrier};
    use std::thread;
    use url::Url;

    #[derive(Clone)]
    struct ProjectionPause {
        entered: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    #[derive(Clone, Default)]
    struct EpochModel {
        accepted_auth_results: usize,
        accepted_descendants: usize,
        accepted_resource_results: usize,
        projected_addons: Vec<Url>,
        projection_pause: Option<ProjectionPause>,
        projection_persistences: usize,
        projection_changes: bool,
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    enum EpochField {
        Ctx,
    }

    impl Model<TestEnv> for EpochModel {
        type Field = EpochField;

        fn update(&mut self, msg: &Msg) -> (Vec<Effect>, Vec<Self::Field>) {
            match msg {
                Msg::Action(Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(addons))) => {
                    let changed = self.projection_changes;
                    self.projection_changes = false;
                    if changed {
                        self.projected_addons = addons
                            .iter()
                            .map(|addon| addon.transport_url.clone())
                            .collect();
                    }
                    if let Some(pause) = self.projection_pause.take() {
                        pause.entered.wait();
                        pause.resume.wait();
                    }
                    let transport_urls = if changed {
                        self.projected_addons.clone()
                    } else {
                        vec![]
                    };
                    let mut effects = vec![Effect::Msg(Box::new(Msg::Event(
                        Event::AddonProjectionReplacedLocal { transport_urls },
                    )))];
                    if changed {
                        effects.push(Effect::Msg(Box::new(Msg::Internal(
                            Internal::ProfileChanged,
                        ))));
                    }
                    let fields = if changed {
                        vec![EpochField::Ctx]
                    } else {
                        vec![]
                    };
                    (effects, fields)
                }
                Msg::Action(Action::Unload) => {
                    (vec![Effect::Msg(Box::new(resource_result()))], vec![])
                }
                Msg::Internal(Internal::ResourceRequestResult(_, _)) => {
                    self.accepted_resource_results += 1;
                    (
                        vec![Effect::Msg(Box::new(Msg::Internal(
                            Internal::PullNotifications,
                        )))],
                        vec![EpochField::Ctx],
                    )
                }
                Msg::Internal(Internal::CtxAuthResult(_, _)) => {
                    self.accepted_auth_results += 1;
                    (vec![], vec![EpochField::Ctx])
                }
                Msg::Internal(Internal::ProfileChanged) => {
                    self.projection_persistences += 1;
                    (vec![], vec![])
                }
                Msg::Internal(Internal::PullNotifications) => {
                    self.accepted_descendants += 1;
                    (vec![], vec![])
                }
                _ => (vec![], vec![]),
            }
        }

        fn update_field(
            &mut self,
            msg: &Msg,
            _field: &Self::Field,
        ) -> (Vec<Effect>, Vec<Self::Field>) {
            self.update(msg)
        }
    }

    fn resource_result() -> Msg {
        Msg::Internal(Internal::ResourceRequestResult(
            ResourceRequest::new(
                Url::parse("https://addon.example/manifest.json").unwrap(),
                ResourcePath::without_extra("stream", "series", "tt1:1:1"),
            ),
            Box::new(Ok(ResourceResponse::Streams { streams: vec![] })),
        ))
    }

    fn auth_result() -> Msg {
        Msg::Internal(Internal::CtxAuthResult(
            AuthRequest::LoginWithToken {
                token: "old-owner-token".to_owned(),
            },
            Err(CtxError::Other(OtherError::UserNotLoggedIn)),
        ))
    }

    #[test]
    fn every_local_addon_mutation_action_is_an_owner_boundary() {
        let descriptor = crate::constants::OFFICIAL_ADDONS[0].clone();
        for action in [
            ActionCtx::InstallAddonLocal(descriptor.clone()),
            ActionCtx::UpgradeAddonLocal(descriptor.clone()),
            ActionCtx::UninstallAddonLocal(descriptor.clone()),
            ActionCtx::ReplaceAddonLocal {
                old: descriptor.clone(),
                new: descriptor.clone(),
            },
            ActionCtx::ReplaceAddonProjectionLocal(vec![descriptor.clone()]),
        ] {
            assert!(is_local_addon_mutation(&Action::Ctx(action)));
        }
        assert!(!is_local_addon_mutation(&Action::Ctx(
            ActionCtx::InstallAddon(descriptor)
        )));
    }

    #[test]
    fn changed_projection_rejects_in_flight_addon_result_for_every_model() {
        let (runtime, _rx) = Runtime::<TestEnv, _>::new(
            EpochModel {
                projection_changes: true,
                ..Default::default()
            },
            vec![],
            8,
        );
        let old_epoch = runtime.addon_epoch.load(Ordering::SeqCst);

        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![])),
        });
        runtime.handle_effect_output(
            resource_result(),
            AddonEffectContext {
                epoch: old_epoch,
                fenced: false,
            },
        );

        assert_eq!(
            runtime.model().unwrap().accepted_resource_results,
            0,
            "a pre-projection player/catalog/meta result must never reach model update"
        );
    }

    #[test]
    fn noop_projection_still_rejects_in_flight_addon_result() {
        let (runtime, _rx) = Runtime::<TestEnv, _>::new(EpochModel::default(), vec![], 8);
        let epoch = runtime.addon_epoch.load(Ordering::SeqCst);

        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![])),
        });
        runtime.handle_effect_output(
            resource_result(),
            AddonEffectContext {
                epoch,
                fenced: false,
            },
        );

        assert_eq!(runtime.model().unwrap().accepted_resource_results, 0);
    }

    #[test]
    fn local_projection_rejects_in_flight_auth_result_before_model_update() {
        let (runtime, _rx) = Runtime::<TestEnv, _>::new(EpochModel::default(), vec![], 8);
        let epoch = runtime.addon_epoch.load(Ordering::SeqCst);

        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![])),
        });
        runtime.handle_effect_output(
            auth_result(),
            AddonEffectContext {
                epoch,
                fenced: false,
            },
        );

        assert_eq!(
            runtime.model().unwrap().accepted_auth_results,
            0,
            "an auth result carrying the old add-on collection must not replace the new owner"
        );
    }

    #[test]
    fn addon_result_plan_keeps_pre_projection_epoch_when_effect_handling_is_delayed() {
        let (runtime, _rx) = Runtime::<TestEnv, _>::new(EpochModel::default(), vec![], 8);
        let planned = runtime.plan_action(RuntimeAction {
            field: None,
            action: Action::Unload,
        });

        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![])),
        });
        runtime.handle_effects(planned);

        assert_eq!(
            runtime.model().unwrap().accepted_resource_results,
            0,
            "an add-on result planned before the owner boundary must not inherit the new epoch"
        );
    }

    #[test]
    fn addon_result_descendants_keep_origin_epoch_when_effect_handling_is_delayed() {
        let (runtime, mut rx) = Runtime::<TestEnv, _>::new(EpochModel::default(), vec![], 8);
        let epoch = runtime.addon_epoch.load(Ordering::SeqCst);
        let planned = runtime
            .plan_internal(
                &resource_result(),
                AddonEffectContext {
                    epoch,
                    fenced: false,
                },
            )
            .expect("the result is current before the forced owner transition");

        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![])),
        });
        runtime.handle_effects(planned);

        let model = runtime.model().unwrap();
        assert_eq!(model.accepted_resource_results, 1);
        assert_eq!(
            model.accepted_descendants, 0,
            "a descendant of the old result must not be rebound to the new epoch"
        );
        drop(model);
        let mut emitted_stale_state = false;
        while let Some(Some(event)) = rx.next().now_or_never() {
            emitted_stale_state |= matches!(event, RuntimeEvent::NewState(_, _));
        }
        assert!(
            !emitted_stale_state,
            "the stale result must not emit NewState after the owner transition"
        );
    }

    #[test]
    fn current_addon_result_preserves_state_and_descendant_effects() {
        let (runtime, mut rx) = Runtime::<TestEnv, _>::new(EpochModel::default(), vec![], 8);
        let epoch = runtime.addon_epoch.load(Ordering::SeqCst);

        runtime.handle_effect_output(
            resource_result(),
            AddonEffectContext {
                epoch,
                fenced: false,
            },
        );

        let model = runtime.model().unwrap();
        assert_eq!(model.accepted_resource_results, 1);
        assert_eq!(model.accepted_descendants, 1);
        drop(model);
        assert!(matches!(
            rx.next().now_or_never(),
            Some(Some(RuntimeEvent::NewState(
                fields,
                EpochModel {
                    accepted_auth_results: 0,
                    accepted_descendants: 0,
                    accepted_resource_results: 1,
                    ..
                }
            ))) if fields == vec![EpochField::Ctx]
        ));
    }

    #[test]
    fn committed_projection_drains_before_a_following_noop_projection() {
        let entered = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let target = crate::constants::OFFICIAL_ADDONS[0].clone();
        let (runtime, mut rx) = Runtime::<TestEnv, _>::new(
            EpochModel {
                projected_addons: vec![
                    Url::parse("https://old-owner.example/manifest.json").unwrap()
                ],
                projection_pause: Some(ProjectionPause {
                    entered: entered.clone(),
                    resume: resume.clone(),
                }),
                projection_changes: true,
                ..Default::default()
            },
            vec![],
            8,
        );
        let runtime = Arc::new(runtime);

        let first_runtime = runtime.clone();
        let first_target = target.clone();
        let first = thread::spawn(move || {
            first_runtime.dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![first_target])),
            });
        });
        entered.wait();

        let (second_started_tx, second_started_rx) = mpsc::channel();
        let (second_done_tx, second_done_rx) = mpsc::channel();
        let second_runtime = runtime.clone();
        let second = thread::spawn(move || {
            second_started_tx.send(()).unwrap();
            second_runtime.dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![target])),
            });
            second_done_tx.send(()).unwrap();
        });
        second_started_rx.recv().unwrap();
        assert!(matches!(
            second_done_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        resume.wait();
        first.join().unwrap();
        second.join().unwrap();
        second_done_rx.recv().unwrap();

        let model = runtime.model().unwrap();
        assert_eq!(
            model.projected_addons,
            vec![crate::constants::OFFICIAL_ADDONS[0].transport_url.clone()]
        );
        assert_eq!(
            model.projection_persistences, 1,
            "the changed projection must persist exactly once"
        );
        drop(model);

        let mut new_states = 0;
        let mut receipt_sizes = vec![];
        while let Some(Some(event)) = rx.next().now_or_never() {
            match event {
                RuntimeEvent::NewState(_, _) => new_states += 1,
                RuntimeEvent::CoreEvent(Event::AddonProjectionReplacedLocal { transport_urls }) => {
                    receipt_sizes.push(transport_urls.len())
                }
                RuntimeEvent::CoreEvent(_) => {}
            }
        }
        assert_eq!(new_states, 1);
        assert_eq!(
            receipt_sizes,
            vec![1, 0],
            "the changed receipt must precede the following no-op receipt"
        );
    }
}
