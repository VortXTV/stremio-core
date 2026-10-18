//! D1 transport prerequisite. No publication, retry, authority issuance or Owner policy.
use futures::channel::oneshot;

use crate::runtime::msg::Event;

/// Exactly the original logical Profile/Library outcomes, never physical member ACKs.
pub type CompletionEvents = [Option<Event>; 2];

/// The sequential worker must return after handing off Pending; it must not await it.
pub enum PrefixCompletion {
    Final(CompletionEvents),
    Pending(PendingCompletion),
}

/// Non-Clone, non-serializable, one original receiver. No Runtime or authority refresh API.
pub struct PendingCompletion {
    receiver: oneshot::Receiver<CompletionEvents>,
}

/// Non-Clone and consumed on submission. A later Unresolved operation must not replace it.
/// The actual adapter may submit only outcomes justified by the original exact receipt.
pub struct CompletionPublisher {
    sender: oneshot::Sender<CompletionEvents>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionSubmission {
    Queued,
    /// Observer loss, NOT definite non-publication and never permission to replay.
    ReceiverGone,
}

impl PendingCompletion {
    pub fn channel() -> (CompletionPublisher, Self) {
        let (sender, receiver) = oneshot::channel();
        (CompletionPublisher { sender }, Self { receiver })
    }

    pub(crate) async fn receive(self) -> Option<CompletionEvents> {
        // Dropping the original publisher (retirement/cancellation) emits no success.
        self.receiver.await.ok()
    }
}

impl CompletionPublisher {
    pub fn submit(self, events: CompletionEvents) -> CompletionSubmission {
        match self.sender.send(events) {
            Ok(()) => CompletionSubmission::Queued,
            Err(_undelivered) => CompletionSubmission::ReceiverGone,
        }
    }
}
