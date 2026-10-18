//! Source-only recorded transport candidate. No storage/attempt authority is issued here.
//! Queue insertion is the event's observation commit. Receiver close shares that lock.
use futures::{stream::FusedStream, Stream};
use std::collections::LinkedList;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CommitError {
    Closed,
    Exhausted,
    Poisoned,
    Consumed,
}

struct Queue<T> {
    items: LinkedList<T>,
    count: usize,
    next: u64,
    closed: Option<CommitError>,
    waiter: Option<Waker>,
}
struct Shared<T> {
    queue: Mutex<Queue<T>>,
}

// Every prepared event allocates both list nodes BEFORE commit/authority acquisition.
// append splices their ownership; no allocation or payload/Waker Drop is needed there.
struct ReleaseSlot<T> {
    rejected: LinkedList<T>,
    wake: Option<Waker>,
}
pub(super) struct Prepared<T> {
    item: LinkedList<T>,
    release: LinkedList<ReleaseSlot<T>>,
}
impl<T> Prepared<T> {
    pub(super) fn new(item: T) -> Self {
        let mut items = LinkedList::new();
        items.push_back(item);
        let mut release = LinkedList::new();
        release.push_back(ReleaseSlot {
            rejected: LinkedList::new(),
            wake: None,
        });
        Self {
            item: items,
            release,
        }
    }
}

/// Detached while Runtime transition is held, destroyed only AFTER its guard releases.
pub(super) struct Released<T> {
    slots: LinkedList<ReleaseSlot<T>>,
}
impl<T> Drop for Released<T> {
    fn drop(&mut self) {
        let already_unwinding = std::thread::panicking();
        let mut first_panic = None;
        while let Some(mut slot) = self.slots.pop_front() {
            if let Some(waker) = slot.wake.take() {
                // One panicking wake must not abandon other taken waiters. No queue,
                // Runtime, Model or authority guard belongs to this release scope.
                if let Err(error) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                    if first_panic.is_none() {
                        first_panic = Some(error);
                    }
                }
            }
            // A hostile rejected payload Drop must not abandon another taken waiter.
            // This does not make arbitrary double-panicking destructors recoverable.
            if let Err(error) = catch_unwind(AssertUnwindSafe(|| drop(slot))) {
                if first_panic.is_none() {
                    first_panic = Some(error);
                }
            }
        }
        if let Some(error) = first_panic {
            if !already_unwinding {
                resume_unwind(error);
            }
            // Preserve an existing unwind rather than start a second wake panic.
            // Arbitrarily panicking payload/panic-payload Drop may still abort, as in Rust.
            drop(error);
        }
    }
}

struct Producer<T> {
    shared: Arc<Shared<T>>,
    deferred: Mutex<LinkedList<ReleaseSlot<T>>>,
}
impl<T> Producer<T> {
    fn detach(&self) -> Released<T> {
        let mut pending = self
            .deferred
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        Released {
            slots: std::mem::take(&mut *pending),
        }
    }
}
impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Runtime operations borrow their sender; CompletionTarget owns its sender
        // until after its guarded block. Thus this LAST producer cannot be destroyed
        // under those operations' guards. This does not constrain caller-owned locks.
        let wake = {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if queue.closed.is_none() {
                queue.closed = Some(CommitError::Closed);
            }
            queue.waiter.take()
        };
        // Preserve drainable committed nodes. Detached release payloads never enter
        // the queue again; this is termination, not retry/publication rollback.
        let released = self.detach();
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(released);
    }
}

pub(super) struct RecordedSender<T> {
    producer: Arc<Producer<T>>,
}
impl<T> Clone for RecordedSender<T> {
    fn clone(&self) -> Self {
        Self {
            producer: Arc::clone(&self.producer),
        }
    }
}

/// A single consumer of the SAME committed queue; no downstream mpsc/flusher exists.
/// No strict backlog cap is imposed. Memory consumption grows with undrained output.
pub struct RecordedReceiver<T> {
    shared: Arc<Shared<T>>,
    terminated: bool,
}

pub(super) fn channel<T>() -> (RecordedSender<T>, RecordedReceiver<T>) {
    let shared = Arc::new(Shared {
        queue: Mutex::new(Queue {
            items: LinkedList::new(),
            count: 0,
            next: 0,
            closed: None,
            waiter: None,
        }),
    });
    let producer = Arc::new(Producer {
        shared: Arc::clone(&shared),
        deferred: Mutex::new(LinkedList::new()),
    });
    (
        RecordedSender { producer },
        RecordedReceiver {
            shared,
            terminated: false,
        },
    )
}

impl<T> RecordedSender<T> {
    /// Caller serializes ALL commits with the owning Runtime transition. The prepared
    /// item remains owned on rejection. No callbacks, allocation or arbitrary Drop.
    pub(super) fn commit(&self, prepared: &mut Prepared<T>) -> Result<(), CommitError> {
        if prepared.item.len() != 1 || prepared.release.len() != 1 {
            return Err(CommitError::Consumed);
        }
        let (mut queue, poisoned) = match self.producer.shared.queue.lock() {
            Ok(guard) => (guard, false),
            Err(error) => (error.into_inner(), true),
        };
        if poisoned {
            queue.closed = Some(CommitError::Poisoned);
        }
        let outcome = if let Some(reason) = queue.closed {
            Err(reason)
        } else if let (Some(next), Some(count)) =
            (queue.next.checked_add(1), queue.count.checked_add(1))
        {
            queue.items.append(&mut prepared.item);
            queue.next = next;
            queue.count = count;
            Ok(())
        } else {
            queue.closed = Some(CommitError::Exhausted);
            Err(CommitError::Exhausted)
        };
        // Prepared is private and constructed with precisely one release slot.
        // Avoid an assertion/panic while holding the queue or authority.
        if let Some(slot) = prepared.release.front_mut() {
            slot.wake = queue.waiter.take();
        }
        drop(queue);
        if let Some(slot) = prepared.release.front_mut() {
            slot.rejected.append(&mut prepared.item);
        }
        let mut pending = self
            .producer
            .deferred
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pending.append(&mut prepared.release);
        outcome
    }

    /// Must be called while transition is still held, then RELEASE the returned
    /// ownership after transition. Detach-before-unlock prevents stealing a later
    /// concurrent operation's wake and invoking it while that operation is guarded.
    pub(super) fn detach(&self) -> Released<T> {
        self.producer.detach()
    }

    // Test-only Runtime close, called outside transition after pending cancellation.
    #[cfg(test)]
    pub(super) fn close(&self) {
        let wake = {
            let mut queue = self
                .producer
                .shared
                .queue
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if queue.closed.is_none() {
                queue.closed = Some(CommitError::Closed);
            }
            queue.waiter.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

impl<T> RecordedReceiver<T> {
    /// Close forbids new commits but retains every node already committed for drain.
    pub fn close(&mut self) {
        let old_waiter = {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if queue.closed.is_none() {
                queue.closed = Some(CommitError::Closed);
            }
            queue.waiter.take()
        };
        // &mut self gives this caller the consumer; no other safe poll is concurrent.
        // Do not invoke a receiver wake from receiver close, matching that ownership.
        drop(old_waiter);
    }
}

impl<T> Stream for RecordedReceiver<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        // RawWaker clone may execute arbitrary code; it occurs before queue lock.
        let mut next_waiter = Some(cx.waker().clone());
        let mut head = LinkedList::new();
        let old_waiter;
        let finished;
        {
            let mut queue = match self.shared.queue.lock() {
                Ok(guard) => guard,
                Err(error) => {
                    let mut guard = error.into_inner();
                    guard.closed = Some(CommitError::Poisoned);
                    guard
                }
            };
            if !queue.items.is_empty() {
                // Detach the first LIST NODE without freeing it under the lock.
                let rest = queue.items.split_off(1);
                head = std::mem::replace(&mut queue.items, rest);
                queue.count -= 1;
                old_waiter = queue.waiter.take();
                finished = false;
            } else if queue.closed.is_some() {
                old_waiter = queue.waiter.take();
                finished = true;
            } else {
                old_waiter = std::mem::replace(&mut queue.waiter, next_waiter.take());
                finished = false;
            }
        }
        // Old and unused RawWaker Drop are off-lock; either may reenter the producer.
        drop(old_waiter);
        drop(next_waiter);
        if let Some(item) = head.pop_front() {
            Poll::Ready(Some(item))
        } else if finished {
            self.terminated = true;
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
impl<T> FusedStream for RecordedReceiver<T> {
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}
impl<T> Drop for RecordedReceiver<T> {
    fn drop(&mut self) {
        let (items, waiter) = {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if queue.closed.is_none() {
                queue.closed = Some(CommitError::Closed);
            }
            queue.count = 0;
            (std::mem::take(&mut queue.items), queue.waiter.take())
        };
        drop(waiter);
        drop(items);
    }
}

// A Runtime transition owns this guard. All synchronous recursive output shares the
// sender's deferred list; it is detached BEFORE unlock and executed AFTER unlock.
pub(super) struct RecordedTransition<'a, T> {
    guard: Option<MutexGuard<'a, ()>>,
    sender: Option<&'a RecordedSender<T>>,
}
impl<'a, T> RecordedTransition<'a, T> {
    pub(super) fn new(guard: MutexGuard<'a, ()>, sender: Option<&'a RecordedSender<T>>) -> Self {
        Self {
            guard: Some(guard),
            sender,
        }
    }
}
impl<T> Drop for RecordedTransition<'_, T> {
    fn drop(&mut self) {
        let released = self.sender.map(RecordedSender::detach);
        drop(self.guard.take());
        drop(released);
    }
}

#[cfg(test)]
#[path = "recorded_output_tests.rs"]
mod tests;
