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
use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard};

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

#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Runtime<E: Env, M: Model<E>> {
    model: Arc<RwLock<M>>,
    tx: Sender<RuntimeEvent<E, M>>,
    addon_epoch: Arc<AtomicU64>,
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
            env: PhantomData,
        };
        runtime.handle_effects(effects, vec![]);
        (runtime, rx)
    }
    pub fn model(&self) -> LockResult<RwLockReadGuard<'_, M>> {
        self.model.read()
    }
    pub fn dispatch(&self, action: RuntimeAction<E, M>) {
        let invalidates_addon_results =
            action.field.is_none() && is_local_addon_mutation(&action.action);
        let (effects, fields) = {
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
            (effects, fields)
        };
        self.handle_effects(effects, fields);
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
    fn handle_effects(&self, effects: Vec<Effect>, fields: Vec<M::Field>) {
        if !fields.is_empty() {
            #[cfg(test)]
            let model = self.model.read().expect("model read failed");
            self.emit(RuntimeEvent::<E, M>::NewState(
                fields,
                #[cfg(test)]
                model.to_owned(),
            ));
        };
        effects
            .into_iter()
            .for_each(enclose!((self.clone() => runtime) move |effect| {
                match effect {
                    Effect::Msg(msg) => {
                        runtime.handle_effect_output(*msg, None);
                    }
                    Effect::Future(EffectFuture::Sequential(future)) => {
                        let addon_epoch = runtime.addon_epoch.load(Ordering::SeqCst);
                        E::exec_sequential(future.then(enclose!((runtime) move |msg| async move {
                            runtime.handle_effect_output(msg, Some(addon_epoch));
                        })))
                    },
                    Effect::Future(EffectFuture::Concurrent(future)) => {
                        let addon_epoch = runtime.addon_epoch.load(Ordering::SeqCst);
                        E::exec_concurrent(future.then(enclose!((runtime) move |msg| async move {
                            runtime.handle_effect_output(msg, Some(addon_epoch));
                        })))
                    }
                }
            }));
    }
    fn handle_effect_output(&self, msg: Msg, effect_addon_epoch: Option<u64>) {
        match msg {
            Msg::Event(event) => {
                self.emit(RuntimeEvent::CoreEvent(event));
            }
            Msg::Internal(_) => {
                let mut model = self.model.write().expect("model write failed");
                if effect_addon_epoch.is_some_and(|effect_addon_epoch| {
                    is_addon_result(&msg)
                        && effect_addon_epoch != self.addon_epoch.load(Ordering::SeqCst)
                }) {
                    return;
                }
                let (effects, fields) = model.update(&msg);
                drop(model);
                self.handle_effects(effects, fields);
            }
            Msg::Action(_) => {
                panic!("effects are not allowed to resolve with action");
            }
        }
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
            Internal::AddonsAPIResult(_, _)
                | Internal::ResourceRequestResult(_, _)
                | Internal::ManifestRequestResult(_, _)
                | Internal::NotificationsRequestResult(_, _)
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::addon::{ResourcePath, ResourceRequest, ResourceResponse};
    use crate::unit_tests::TestEnv;
    use serde::{Deserialize, Serialize};
    use url::Url;

    #[derive(Clone, Default)]
    struct EpochModel {
        accepted_resource_results: usize,
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
                Msg::Action(Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(_)))
                    if self.projection_changes =>
                {
                    self.projection_changes = false;
                    (vec![], vec![EpochField::Ctx])
                }
                Msg::Internal(Internal::ResourceRequestResult(_, _)) => {
                    self.accepted_resource_results += 1;
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
        runtime.handle_effect_output(resource_result(), Some(old_epoch));

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
        runtime.handle_effect_output(resource_result(), Some(epoch));

        assert_eq!(runtime.model().unwrap().accepted_resource_results, 0);
    }
}
