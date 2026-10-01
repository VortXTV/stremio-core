use crate::runtime::msg::{Action, ActionCtx, Event, Internal, Msg};
use crate::runtime::{Effect, EffectFuture, Env, Model};
use derivative::Derivative;
use enclose::enclose;
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
    effects: Vec<Effect>,
    fields: Vec<F>,
    addon_context: AddonEffectContext,
}

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Runtime<E: Env, M: Model<E>> {
    model: Arc<RwLock<M>>,
    tx: Sender<RuntimeEvent<E, M>>,
    addon_epoch: Arc<AtomicU64>,
    transition: Arc<Mutex<()>>,
    env: PhantomData<E>,
}

impl<E, M> Runtime<E, M>
where
    E: Env + Send + 'static,
    M: Model<E> + Send + Sync + 'static,
{
    pub fn new(
        model: M,
        effects: Vec<Effect>,
        buffer: usize,
    ) -> (Self, Receiver<RuntimeEvent<E, M>>) {
        let (tx, rx) = channel(buffer);
        let model = Arc::new(RwLock::new(model));
        let runtime = Runtime {
            model,
            tx,
            addon_epoch: Arc::new(AtomicU64::new(0)),
            transition: Arc::new(Mutex::new(())),
            env: PhantomData,
        };
        {
            let _transition = runtime.transition.lock().expect("transition lock failed");
            runtime.handle_effects(PlannedEffects {
                effects,
                fields: vec![],
                addon_context: AddonEffectContext {
                    epoch: 0,
                    fenced: false,
                },
            });
        }
        (runtime, rx)
    }
    pub fn model(&self) -> LockResult<RwLockReadGuard<'_, M>> {
        self.model.read()
    }
    pub fn dispatch(&self, action: RuntimeAction<E, M>) {
        let _transition = self.transition.lock().expect("transition lock failed");
        let planned = self.plan_action(action);
        self.handle_effects(planned);
    }
    fn plan_action(&self, action: RuntimeAction<E, M>) -> PlannedEffects<M::Field> {
        let invalidates_addon_results =
            action.field.is_none() && is_local_addon_mutation(&action.action);
        let mut model = self.model.write().expect("model write failed");
        let (effects, fields) = match action {
            RuntimeAction {
                field: Some(field),
                action,
            } => model.update_field(&Msg::Action(action), &field),
            RuntimeAction { action, .. } => model.update(&Msg::Action(action)),
        };
        if invalidates_addon_results {
            self.addon_epoch.fetch_add(1, Ordering::SeqCst);
        }
        let addon_context = AddonEffectContext {
            epoch: self.addon_epoch.load(Ordering::SeqCst),
            fenced: false,
        };
        PlannedEffects {
            effects,
            fields,
            addon_context,
        }
    }

    /// Run one host-owned model transaction and publish the returned state changes.
    ///
    /// This is deliberately lower-level than [`Self::dispatch`]: platform bridges sometimes need
    /// to restore already-authenticated persisted state whose authoritative timestamp must not be
    /// replaced by a local `Env::now()` action. The caller performs all validation and persistence
    /// while the model write lock is held, then returns the exact effects and model fields that
    /// should be published. Effects and `RuntimeEvent::NewState` are handled only after the model
    /// lock is released, while the shared transition lock remains held, matching `dispatch`'s
    /// ordering and preserving local add-on owner/result fencing.
    ///
    /// A caller can return any result type (including `Result`) and an empty effects/fields pair to
    /// fail closed without publishing a state change.
    pub fn transact<R>(
        &self,
        transaction: impl FnOnce(&mut M) -> (R, Vec<Effect>, Vec<M::Field>),
    ) -> R {
        let _transition = self.transition.lock().expect("transition lock failed");
        let (result, effects, fields) = {
            let mut model = self.model.write().expect("model write failed");
            transaction(&mut model)
        };
        self.handle_effects(PlannedEffects {
            effects,
            fields,
            addon_context: AddonEffectContext {
                epoch: self.addon_epoch.load(Ordering::SeqCst),
                fenced: false,
            },
        });
        result
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
    fn handle_effects(&self, planned: PlannedEffects<M::Field>) {
        let PlannedEffects {
            effects,
            fields,
            addon_context,
        } = planned;
        if addon_context.fenced && addon_context.epoch != self.addon_epoch.load(Ordering::SeqCst) {
            return;
        }
        if !fields.is_empty() {
            if addon_context.fenced {
                let model = self.model.read().expect("model read failed");
                if addon_context.epoch != self.addon_epoch.load(Ordering::SeqCst) {
                    return;
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
        };
        effects
            .into_iter()
            .for_each(enclose!((self.clone() => runtime) move |effect| {
                match effect {
                    Effect::Msg(msg) => {
                        runtime.handle_effect_output(*msg, addon_context);
                    }
                    Effect::Future(EffectFuture::Sequential(future)) => {
                        E::exec_sequential(future.then(enclose!((runtime) move |msg| async move {
                            runtime.handle_scheduled_effect_output(msg, addon_context);
                        })))
                    },
                    Effect::Future(EffectFuture::Concurrent(future)) => {
                        E::exec_concurrent(future.then(enclose!((runtime) move |msg| async move {
                            runtime.handle_scheduled_effect_output(msg, addon_context);
                        })))
                    }
                }
            }));
    }
    fn handle_scheduled_effect_output(&self, msg: Msg, addon_context: AddonEffectContext) {
        let _transition = self.transition.lock().expect("transition lock failed");
        self.handle_effect_output(msg, addon_context);
    }
    fn handle_effect_output(&self, msg: Msg, addon_context: AddonEffectContext) {
        match msg {
            Msg::Event(event) => {
                if addon_context.fenced {
                    let model = self.model.read().expect("model read failed");
                    if addon_context.epoch != self.addon_epoch.load(Ordering::SeqCst) {
                        return;
                    }
                    self.emit(RuntimeEvent::CoreEvent(event));
                    drop(model);
                } else {
                    self.emit(RuntimeEvent::CoreEvent(event));
                }
            }
            Msg::Internal(_) => {
                if let Some(planned) = self.plan_internal(&msg, addon_context) {
                    self.handle_effects(planned);
                }
            }
            Msg::Action(_) => {
                panic!("effects are not allowed to resolve with action");
            }
        }
    }
    fn plan_internal(
        &self,
        msg: &Msg,
        addon_context: AddonEffectContext,
    ) -> Option<PlannedEffects<M::Field>> {
        let addon_context = AddonEffectContext {
            fenced: addon_context.fenced || is_addon_result(msg),
            ..addon_context
        };
        let mut model = self.model.write().expect("model write failed");
        if addon_context.fenced && addon_context.epoch != self.addon_epoch.load(Ordering::SeqCst) {
            return None;
        }
        let (effects, fields) = model.update(msg);
        let addon_context = if addon_context.fenced {
            addon_context
        } else {
            AddonEffectContext {
                epoch: self.addon_epoch.load(Ordering::SeqCst),
                fenced: false,
            }
        };
        Some(PlannedEffects {
            effects,
            fields,
            addon_context,
        })
    }
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
    fn host_transaction_retains_transition_lock_and_current_addon_epoch() {
        let (runtime, _rx) = Runtime::<TestEnv, _>::new(EpochModel::default(), vec![], 8);
        let prior_epoch = runtime.addon_epoch.load(Ordering::SeqCst);
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![])),
        });
        let current_epoch = runtime.addon_epoch.load(Ordering::SeqCst);
        assert_ne!(prior_epoch, current_epoch);

        runtime.transact(|_model| {
            assert!(matches!(runtime.transition.try_lock(), Err(std::sync::TryLockError::WouldBlock)));
            ((), vec![Effect::Msg(Box::new(resource_result()))], vec![])
        });
        assert_eq!(runtime.addon_epoch.load(Ordering::SeqCst), current_epoch);
        runtime.handle_scheduled_effect_output(auth_result(), AddonEffectContext {
            epoch: prior_epoch,
            fenced: false,
        });
        let model = runtime.model().unwrap();
        assert_eq!(model.accepted_resource_results, 1);
        assert_eq!(model.accepted_descendants, 1);
        assert_eq!(model.accepted_auth_results, 0, "host transaction must not revive prior-owner effects");
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

#[cfg(test)]
mod transaction_tests {
    use futures::{FutureExt, StreamExt};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        runtime::{msg::Internal, Effects, Model},
        unit_tests::TestEnv,
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TransactionModel {
        value: u8,
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
    enum TransactionField {
        Value,
    }

    impl Model<TestEnv> for TransactionModel {
        type Field = TransactionField;

        fn update(&mut self, msg: &Msg) -> (Vec<Effect>, Vec<Self::Field>) {
            if matches!(msg, Msg::Internal(Internal::LibraryChanged(true))) {
                // This write proves `transact` released its model lock before handling effects.
                self.value += 1;
                (vec![], vec![TransactionField::Value])
            } else {
                (vec![], vec![])
            }
        }

        fn update_field(&mut self, msg: &Msg, _field: &Self::Field) -> (Vec<Effect>, Vec<Self::Field>) {
            self.update(msg)
        }
    }

    #[test]
    fn transact_publishes_success_and_handles_internal_effects_after_unlock() {
        let _env = TestEnv::reset().expect("test environment lock");
        let (runtime, mut events) = Runtime::<TestEnv, _>::new(TransactionModel { value: 0 }, vec![], 4);

        let result = runtime.transact(|model| {
            model.value = 1;
            (
                Ok::<_, ()>("persisted"),
                Effects::msg(Msg::Internal(Internal::LibraryChanged(true)))
                    .into_iter()
                    .collect(),
                vec![TransactionField::Value],
            )
        });

        assert_eq!(result, Ok("persisted"));
        assert_eq!(runtime.model().expect("model read").value, 2);
        assert!(events.next().now_or_never().is_some(), "transaction publication missing");
        assert!(events.next().now_or_never().is_some(), "internal effect publication missing");
    }

    #[test]
    fn transact_failure_can_leave_memory_and_publication_unchanged() {
        let _env = TestEnv::reset().expect("test environment lock");
        let (runtime, mut events) = Runtime::<TestEnv, _>::new(TransactionModel { value: 7 }, vec![], 4);

        let result = runtime.transact(|_model| {
            (Err::<(), _>("persistence failed"), vec![], vec![])
        });

        assert_eq!(result, Err("persistence failed"));
        assert_eq!(runtime.model().expect("model read").value, 7);
        assert!(events.next().now_or_never().is_none(), "failure must not publish state");
    }
}
