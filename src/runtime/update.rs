use crate::models::ctx::Ctx;
use crate::runtime::msg::Msg;
use crate::runtime::{
    AdmissionOutcome, Effect, EffectScope, Effects, Env, ProtocolRejection, ScopeToken, ScopedPlan,
};
#[cfg(debug_assertions)]
use core::fmt::Debug;
use serde::{Deserialize, Serialize};

pub trait Model<E: Env>: Clone {
    #[cfg(not(debug_assertions))]
    type Field: Send + Sync + Serialize + for<'de> Deserialize<'de>;
    #[cfg(debug_assertions)]
    type Field: Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>;

    fn update(&mut self, msg: &Msg) -> (Vec<Effect>, Vec<Self::Field>);
    fn update_field(&mut self, msg: &Msg, field: &Self::Field) -> (Vec<Effect>, Vec<Self::Field>);

    /// Opt in only after the actual borrowed capture/decline contracts are implemented.
    fn supports_recorded_dispatch(&self) -> bool {
        false
    }
    /// Called only for a new root action, NEVER to repair a missing descendant scope.
    fn root_scope(&self) -> Option<ScopeToken> {
        None
    }
    fn update_scoped(&mut self, msg: &Msg, scope: &EffectScope) -> ScopedPlan<Self::Field> {
        if matches!(scope, EffectScope::Legacy) {
            let (effects, fields) = self.update(msg);
            ScopedPlan::ordinary(effects, fields, scope)
        } else {
            ScopedPlan::reject(
                scope,
                AdmissionOutcome::ProtocolRejected(ProtocolRejection::Unsupported),
            )
        }
    }
    fn update_field_scoped(
        &mut self,
        msg: &Msg,
        field: &Self::Field,
        scope: &EffectScope,
    ) -> ScopedPlan<Self::Field> {
        if matches!(scope, EffectScope::Legacy) {
            let (effects, fields) = self.update_field(msg, field);
            ScopedPlan::ordinary(effects, fields, scope)
        } else {
            ScopedPlan::reject(
                scope,
                AdmissionOutcome::ProtocolRejected(ProtocolRejection::Unsupported),
            )
        }
    }
}

pub trait Update<E: Env> {
    fn update(&mut self, msg: &Msg) -> Effects;
}
pub trait UpdateWithCtx<E: Env> {
    fn update(&mut self, msg: &Msg, ctx: &Ctx) -> Effects;
}
