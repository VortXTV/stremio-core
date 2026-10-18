//! Backend-neutral dispatch transport. No owner, backend, epoch policy or retry lives here.
use std::any::Any;
use std::sync::Arc;

use serde::Serialize;

use crate::models::ctx::CtxError;
use crate::runtime::msg::{Event, Internal, Msg};
use crate::runtime::{Effect, Effects, EnvFuture};
use crate::types::library::{LibraryItem, LibraryItemId};

/// Opaque caller provenance. Clone copies one Arc, never a typed model or history.
/// Deliberately has no Debug/Serialize implementation: it may contain sensitive provenance.
#[derive(Clone)]
pub struct ScopeToken(Arc<dyn Any + Send + Sync>);

impl ScopeToken {
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self(Arc::new(value))
    }
    pub fn get<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
    pub fn same_token(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
pub enum EffectScope {
    Legacy,
    Recorded {
        origin: Option<ScopeToken>,
        coverage: Option<ScopeToken>,
    },
}

impl EffectScope {
    pub fn recorded(origin: ScopeToken) -> Self {
        Self::Recorded {
            origin: Some(origin),
            coverage: None,
        }
    }
    pub fn missing() -> Self {
        Self::Recorded {
            origin: None,
            coverage: None,
        }
    }
    pub fn origin(&self) -> Option<&ScopeToken> {
        match self {
            Self::Recorded { origin, .. } => origin.as_ref(),
            Self::Legacy => None,
        }
    }
    pub fn coverage(&self) -> Option<&ScopeToken> {
        match self {
            Self::Recorded { coverage, .. } => coverage.as_ref(),
            Self::Legacy => None,
        }
    }
    /// A notification can have coverage; its NEW children never inherit that permission.
    pub fn descendant(&self) -> Self {
        match self {
            Self::Legacy => Self::Legacy,
            Self::Recorded { origin, .. } => Self::Recorded {
                origin: origin.clone(),
                coverage: None,
            },
        }
    }
    pub fn same_scope(&self, other: &Self) -> bool {
        fn same(a: &Option<ScopeToken>, b: &Option<ScopeToken>) -> bool {
            match (a, b) {
                (None, None) => true,
                (Some(a), Some(b)) => a.same_token(b),
                _ => false,
            }
        }
        match (self, other) {
            (Self::Legacy, Self::Legacy) => true,
            (
                Self::Recorded {
                    origin: a,
                    coverage: ac,
                },
                Self::Recorded {
                    origin: b,
                    coverage: bc,
                },
            ) => same(a, b) && same(ac, bc),
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionReason {
    Unresolved,
    Retired,
    Poisoned,
    CounterExhausted,
    AuthorityInconsistent,
}

/// A map-shaped value is required inside CtxError's internally tagged enum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AdmissionError {
    pub reason: AdmissionReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeDrop {
    AddonEpoch,
    Missing,
    Foreign,
    StaleOrigin,
    StaleAttempt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolRejection {
    Unsupported,
    WrongSubject,
    WrongScope,
    MissingOutcome,
    InvalidFrame,
    Detached,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// Admitted changed-input path; NOT a guarantee that legacy mtime selected the value.
    AcceptedChange,
    /// The real eq_no_mtime branch, only AFTER owner admission.
    AcceptedNoChange,
    RefusedCurrent(AdmissionReason),
    DroppedScope(ScopeDrop),
    DomainDenied,
    ProtocolRejected(ProtocolRejection),
}

pub enum DirectKind {
    Ungoverned,
    LibraryItem {
        id: LibraryItemId,
        outcome: AdmissionOutcome,
    },
    Other {
        outcome: AdmissionOutcome,
        error_source: Option<Event>,
    },
}

pub struct DirectOutcome {
    pub scope: EffectScope,
    pub kind: DirectKind,
}

impl DirectOutcome {
    pub fn ungoverned(scope: &EffectScope) -> Self {
        Self {
            scope: scope.clone(),
            kind: DirectKind::Ungoverned,
        }
    }
    pub fn library(scope: &EffectScope, id: LibraryItemId, outcome: AdmissionOutcome) -> Self {
        Self {
            scope: scope.clone(),
            kind: DirectKind::LibraryItem { id, outcome },
        }
    }
    pub fn rejected(scope: &EffectScope, outcome: AdmissionOutcome) -> Self {
        Self {
            scope: scope.clone(),
            kind: DirectKind::Other {
                outcome,
                error_source: None,
            },
        }
    }
}

/// Only these four source-proven dependent tails may be represented by a frame.
#[derive(Debug, PartialEq, Eq)]
pub enum LibraryActionKind {
    Added,
    Removed,
    Rewinded,
    NotificationsToggled { dismiss: bool },
}

pub struct LibraryContinuation {
    id: LibraryItemId,
    kind: LibraryActionKind,
}

impl LibraryContinuation {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn kind(&self) -> &LibraryActionKind {
        &self.kind
    }
    pub fn source(&self) -> Event {
        match self.kind {
            LibraryActionKind::Added => Event::LibraryItemAdded {
                id: self.id.clone(),
            },
            LibraryActionKind::Removed => Event::LibraryItemRemoved {
                id: self.id.clone(),
            },
            LibraryActionKind::Rewinded => Event::LibraryItemRewinded {
                id: self.id.clone(),
            },
            LibraryActionKind::NotificationsToggled { .. } => {
                Event::LibraryItemNotificationsToggled {
                    id: self.id.clone(),
                }
            }
        }
    }
    pub fn accepted(self) -> Vec<Msg> {
        let event = Msg::Event(self.source());
        match self.kind {
            LibraryActionKind::Removed => vec![
                Msg::Internal(Internal::DismissNotificationItem(self.id)),
                event,
            ],
            LibraryActionKind::NotificationsToggled { dismiss: true } => vec![
                event,
                Msg::Internal(Internal::DismissNotificationItem(self.id)),
            ],
            _ => vec![event],
        }
    }
}

/// Owned and non-Clone; no borrowed Ctx, Prepared, arbitrary callback or future.
pub struct LibraryUpdateFrame {
    item: LibraryItem,
    continuation: LibraryContinuation,
}

impl LibraryUpdateFrame {
    pub fn new(item: LibraryItem, original_id: LibraryItemId, kind: LibraryActionKind) -> Self {
        Self {
            item,
            continuation: LibraryContinuation {
                id: original_id,
                kind,
            },
        }
    }
    pub fn into_parts(self) -> (LibraryItem, LibraryContinuation) {
        (self.item, self.continuation)
    }
    /// This is both the Legacy expansion and the tiny negative-control desugaring.
    /// Runtime is not duplicated; recorded child admission can remain active in a test.
    pub fn into_legacy(self) -> Effects {
        let (item, continuation) = self.into_parts();
        let mut messages = vec![Msg::Internal(Internal::UpdateLibraryItem(item))];
        messages.extend(continuation.accepted());
        Effects::msgs(messages).unchanged()
    }
}

/// The extension is not added to legacy Effect, so legacy exhaustive consumers stay intact.
pub enum ScopedEffectKind {
    Ordinary(Effect),
    LibraryUpdateThen(LibraryUpdateFrame),
}
pub struct ScopedEffect {
    pub effect: ScopedEffectKind,
    pub scope: EffectScope,
}

impl ScopedEffect {
    pub fn ordinary(effect: Effect, scope: EffectScope) -> Self {
        Self {
            effect: ScopedEffectKind::Ordinary(effect),
            scope,
        }
    }
    pub fn library(frame: LibraryUpdateFrame, scope: EffectScope) -> Self {
        Self {
            effect: ScopedEffectKind::LibraryUpdateThen(frame),
            scope,
        }
    }
}

/// At most two logical Profile/Library completion events, never one per physical member.
/// The future owns original subjects/provenance. Runtime only delivers its events.
pub struct LocalPrefix {
    pub future: EnvFuture<'static, [Option<Event>; 2]>,
}

pub struct ScopedPlan<F> {
    pub effects: Vec<ScopedEffect>,
    pub fields: Vec<F>,
    pub prefix: Option<LocalPrefix>,
    pub direct: DirectOutcome,
}

impl<F> ScopedPlan<F> {
    pub fn ordinary(effects: Vec<Effect>, fields: Vec<F>, scope: &EffectScope) -> Self {
        Self {
            effects: effects
                .into_iter()
                .map(|effect| ScopedEffect::ordinary(effect, scope.descendant()))
                .collect(),
            fields,
            prefix: None,
            direct: DirectOutcome::ungoverned(scope),
        }
    }
    pub fn empty(scope: &EffectScope) -> Self {
        Self::ordinary(vec![], vec![], scope)
    }
    pub fn reject(scope: &EffectScope, outcome: AdmissionOutcome) -> Self {
        let mut plan = Self::empty(scope);
        plan.direct = DirectOutcome::rejected(scope, outcome);
        plan
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclineReason {
    Current(AdmissionReason),
    Scope(ScopeDrop),
    Protocol(ProtocolRejection),
}

/// Never Debug/Serialize: ownership is transferred, not logged or copied into an event.
pub struct DeclinedMutation {
    pub input: Msg,
    pub scope: EffectScope,
    pub reason: DeclineReason,
    pub continuation: Option<LibraryContinuation>,
}

pub type DeclineReceiver = Arc<dyn Fn(DeclinedMutation) + Send + Sync + 'static>;

pub fn admission_error(source: Event, reason: AdmissionReason) -> Event {
    Event::Error {
        error: CtxError::Admission(AdmissionError { reason }),
        source: Box::new(source),
    }
}
