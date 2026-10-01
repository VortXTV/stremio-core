use crate::runtime::msg::{Action, Event, Msg};
use crate::runtime::{Effect, EffectFuture, Env, Model};
use derivative::Derivative;
use enclose::enclose;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::FutureExt;
#[cfg(test)]
use futures::SinkExt;
use serde::Serialize;
use std::marker::PhantomData;
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
            env: PhantomData,
        };
        runtime.handle_effects(effects, vec![]);
        (runtime, rx)
    }
    pub fn model(&self) -> LockResult<RwLockReadGuard<'_, M>> {
        self.model.read()
    }
    pub fn dispatch(&self, action: RuntimeAction<E, M>) {
        let (effects, fields) = {
            let mut model = self.model.write().expect("model write failed");
            match action {
                RuntimeAction {
                    field: Some(field),
                    action,
                } => model.update_field(&Msg::Action(action), &field),
                RuntimeAction { action, .. } => model.update(&Msg::Action(action)),
            }
        };
        self.handle_effects(effects, fields);
    }

    /// Run one host-owned model transaction and publish the returned state changes.
    ///
    /// This is deliberately lower-level than [`Self::dispatch`]: platform bridges sometimes need
    /// to restore already-authenticated persisted state whose authoritative timestamp must not be
    /// replaced by a local `Env::now()` action. The caller performs all validation and persistence
    /// while the model write lock is held, then returns the exact effects and model fields that
    /// should be published. Effects and `RuntimeEvent::NewState` are handled only after the lock is
    /// released, matching `dispatch`'s ordering.
    ///
    /// A caller can return any result type (including `Result`) and an empty effects/fields pair to
    /// fail closed without publishing a state change.
    pub fn transact<R>(
        &self,
        transaction: impl FnOnce(&mut M) -> (R, Vec<Effect>, Vec<M::Field>),
    ) -> R {
        let (result, effects, fields) = {
            let mut model = self.model.write().expect("model write failed");
            transaction(&mut model)
        };
        self.handle_effects(effects, fields);
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
                        runtime.handle_effect_output(*msg);
                    }
                    Effect::Future(EffectFuture::Sequential(future)) => {
                        E::exec_sequential(future.then(enclose!((runtime) move |msg| async move {
                            runtime.handle_effect_output(msg);
                        })))
                    },
                    Effect::Future(EffectFuture::Concurrent(future)) => {
                        E::exec_concurrent(future.then(enclose!((runtime) move |msg| async move {
                            runtime.handle_effect_output(msg);
                        })))
                    }
                }
            }));
    }
    fn handle_effect_output(&self, msg: Msg) {
        match msg {
            Msg::Event(event) => {
                self.emit(RuntimeEvent::CoreEvent(event));
            }
            Msg::Internal(_) => {
                let (effects, fields) =
                    self.model.write().expect("model write failed").update(&msg);
                self.handle_effects(effects, fields);
            }
            Msg::Action(_) => {
                panic!("effects are not allowed to resolve with action");
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
