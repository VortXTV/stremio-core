//! Shared four-action construction, extracted from update_library without changing item semantics.
use std::marker::PhantomData;

use crate::models::ctx::{CtxError, OtherError};
use crate::runtime::msg::{Action, ActionCtx, Event, Msg};
use crate::runtime::{Effects, Env, LibraryActionKind, LibraryUpdateFrame};
use crate::types::library::{LibraryBucket, LibraryItem};

pub enum LibraryActionPlan {
    Update(LibraryUpdateFrame),
    Denied(Event),
}

impl LibraryActionPlan {
    pub fn into_legacy(self) -> Effects {
        match self {
            Self::Update(frame) => frame.into_legacy(),
            Self::Denied(event) => Effects::msg(Msg::Event(event)).unchanged(),
        }
    }
}

/// Planning only: no authoritative merge, storage/API construction or admission claim.
/// Recorded callers must submit the actual child to the guarded common Ctx path.
pub fn plan_library_action<E: Env + 'static>(
    library: &LibraryBucket,
    msg: &Msg,
) -> Option<LibraryActionPlan> {
    let frame = match msg {
        Msg::Action(Action::Ctx(ActionCtx::AddToLibrary(meta_preview))) => {
            let mut item = match library.items.get(&meta_preview.id) {
                Some(item) => LibraryItem::from((meta_preview, item)),
                None => LibraryItem::from((meta_preview, PhantomData::<E>)),
            };
            item.removed = false;
            item.temp = false;
            LibraryUpdateFrame::new(item, meta_preview.id.clone(), LibraryActionKind::Added)
        }
        Msg::Action(Action::Ctx(ActionCtx::RemoveFromLibrary(id))) => {
            let mut item = match library.items.get(id) {
                Some(item) => item.clone(),
                None => return Some(missing(Event::LibraryItemRemoved { id: id.clone() })),
            };
            item.removed = true;
            item.temp = false;
            LibraryUpdateFrame::new(item, id.clone(), LibraryActionKind::Removed)
        }
        Msg::Action(Action::Ctx(ActionCtx::RewindLibraryItem(id))) => {
            let mut item = match library.items.get(id) {
                Some(item) => item.clone(),
                None => return Some(missing(Event::LibraryItemRewinded { id: id.clone() })),
            };
            item.state.time_offset = 0;
            LibraryUpdateFrame::new(item, id.clone(), LibraryActionKind::Rewinded)
        }
        Msg::Action(Action::Ctx(ActionCtx::ToggleLibraryItemNotifications(id, state))) => {
            let mut item = match library.items.get(id) {
                Some(item) => item.clone(),
                None => {
                    return Some(missing(Event::LibraryItemNotificationsToggled {
                        id: id.clone(),
                    }))
                }
            };
            item.state.no_notif = *state;
            LibraryUpdateFrame::new(
                item,
                id.clone(),
                LibraryActionKind::NotificationsToggled { dismiss: *state },
            )
        }
        _ => return None,
    };
    Some(LibraryActionPlan::Update(frame))
}

fn missing(source: Event) -> LibraryActionPlan {
    LibraryActionPlan::Denied(Event::Error {
        error: CtxError::from(OtherError::LibraryItemNotFound),
        source: Box::new(source),
    })
}
