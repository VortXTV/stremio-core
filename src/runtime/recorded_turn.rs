//! Recorded-only caller-owned scheduling. No future payload is stored in Runtime.
//! This is an unwired SOURCE candidate; Legacy execution is not routed through it.
use crate::runtime::handoff::{handoff_recorded, HandoffResult, RecordedTask, SpawnRefusal};
use crate::runtime::{EnvFuture, EnvFutureExt};
use futures::Future;
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordedWorkKind {
    Sequential,
    Concurrent,
    CompletionObserver,
}

pub type RecordedSpawner =
    fn(RecordedWorkKind, RecordedTask<RecordedTurn>) -> Result<(), SpawnRefusal>;

struct SequenceNode {
    state: Mutex<SequenceState>,
}
struct SequenceState {
    finished: bool,
    predecessor_passed: bool,
    passed: bool,
    // Strong backward links preserve cancelled intermediate positions. Forward
    // links are Weak, so cancellation cannot form an ownership cycle.
    previous: Option<Arc<SequenceNode>>,
    next: Option<Weak<SequenceNode>>,
    waiter: Option<Waker>,
}
struct Order {
    next: u64,
    tail: Option<Arc<SequenceNode>>,
}

/// Backend-neutral capability. First-party adapters share ONE instance per
/// sequential lane; creating a fresh instance per dispatch would break ordering.
/// It contains only Send-safe metadata/function pointers, never an EnvFuture.
#[derive(Clone)]
pub struct RecordedExecutor {
    spawn: RecordedSpawner,
    order: Arc<Mutex<Order>>,
}

impl RecordedExecutor {
    /// An adapter must schedule independently runnable tasks. A FIFO worker which
    /// awaits task N+1 before task N is offered is not a valid recorded adapter.
    pub fn new(spawn: RecordedSpawner) -> Self {
        Self {
            spawn,
            order: Arc::new(Mutex::new(Order {
                next: 0,
                tail: None,
            })),
        }
    }

    fn reserve(
        &self,
        kind: RecordedWorkKind,
        future: EnvFuture<'static, RecordedTurn>,
    ) -> RecordedOperation {
        // Allocation is before the order mutex, not a claim about whole-reducer allocation.
        let node = (kind == RecordedWorkKind::Sequential).then(|| {
            Arc::new(SequenceNode {
                state: Mutex::new(SequenceState {
                    finished: false,
                    predecessor_passed: false,
                    passed: false,
                    previous: None,
                    next: None,
                    waiter: None,
                }),
            })
        });
        let (ordinal, predecessor) = {
            let mut order = self.order.lock().unwrap_or_else(|error| error.into_inner());
            match order.next.checked_add(1) {
                Some(next) => {
                    order.next = next;
                    let previous = node.as_ref().and_then(|node| {
                        let previous = std::mem::replace(&mut order.tail, Some(Arc::clone(node)));
                        let passed = match previous.as_ref() {
                            Some(previous) => {
                                node.state
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .previous = Some(Arc::clone(previous));
                                let mut prior = previous
                                    .state
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner());
                                prior.next = Some(Arc::downgrade(node));
                                prior.passed
                            }
                            None => true,
                        };
                        // A concurrently finishing predecessor may already have
                        // set this bit. Never overwrite that progress with false.
                        node.state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .predecessor_passed |= passed;
                        previous
                    });
                    (Some(next), previous)
                }
                None => (None, None),
            }
        };
        // On exhaustion all owned input remains in this unschedulable operation.
        // No counter wraps, no fresh lane is substituted, and nothing is retried.
        RecordedOperation {
            ordinal,
            kind,
            executor: self.clone(),
            future,
            predecessor,
            finish: node.map(|node| Finish { node: Some(node) }),
        }
    }
}

/// Returned after guards release, including a caught turn panic. A panic is data
/// for the caller, not permission to replay or a claim that no mutation occurred.
#[must_use = "retain and inspect the outbox; admitted work must not be silently discarded"]
pub struct RecordedTurn {
    operations: Vec<RecordedOperation>,
    panics: Vec<Box<dyn Any + Send + 'static>>,
}
impl RecordedTurn {
    pub fn empty() -> Self {
        Self {
            operations: vec![],
            panics: vec![],
        }
    }
    pub(crate) fn push(
        &mut self,
        executor: &RecordedExecutor,
        kind: RecordedWorkKind,
        future: EnvFuture<'static, RecordedTurn>,
    ) {
        self.operations.push(executor.reserve(kind, future));
    }
    pub(crate) fn capture_panic(&mut self, panic: Option<Box<dyn Any + Send + 'static>>) {
        if let Some(panic) = panic {
            self.panics.push(panic);
        }
    }
    pub fn into_parts(self) -> (Vec<RecordedOperation>, Vec<Box<dyn Any + Send + 'static>>) {
        (self.operations, self.panics)
    }
}

/// Non-Clone, original capture and ordinal. The owner holds it outside application
/// guards. Explicit caller cancellation is dropping this owner; no automatic retry.
#[must_use = "handoff or explicitly retain/cancel the original operation off guards"]
pub struct RecordedOperation {
    ordinal: Option<u64>,
    kind: RecordedWorkKind,
    executor: RecordedExecutor,
    future: EnvFuture<'static, RecordedTurn>,
    predecessor: Option<Arc<SequenceNode>>,
    finish: Option<Finish>,
}

#[must_use = "retain the receipt until its owned outcome is handled"]
pub struct RecordedSubmission {
    pub ordinal: u64,
    pub kind: RecordedWorkKind,
    pub result: HandoffResult<RecordedTurn>,
}

impl RecordedOperation {
    pub fn ordinal(&self) -> Option<u64> {
        self.ordinal
    }
    pub fn kind(&self) -> RecordedWorkKind {
        self.kind
    }

    /// ONLY after transition/Model/authority guards are released. Exhaustion returns
    /// the exact operation. Refusal/panic retains its exact unpolled future in receipt.
    pub fn handoff(self) -> Result<RecordedSubmission, Self> {
        let Some(ordinal) = self.ordinal else {
            return Err(self);
        };
        let Self {
            kind,
            executor,
            future,
            predecessor,
            finish,
            ..
        } = self;
        let ordered = async move {
            // Declared before await: cancellation also releases this lane position,
            // but only when the caller explicitly drops the original owned work.
            let mut finish = finish;
            if let Some(node) = predecessor {
                Predecessor { node }.await;
            }
            let mut turn = future.await;
            if let Some(finish) = finish.as_mut() {
                turn.capture_panic(finish.complete());
            }
            turn
        }
        .boxed_env();
        let result = handoff_recorded(Some(move |task| (executor.spawn)(kind, task)), ordered);
        Ok(RecordedSubmission {
            ordinal,
            kind,
            result,
        })
    }
}

struct Predecessor {
    node: Arc<SequenceNode>,
}
impl Future for Predecessor {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut incoming = Some(cx.waker().clone());
        let (done, old) = {
            let mut state = self
                .node
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.passed {
                (true, state.waiter.take())
            } else {
                (false, std::mem::replace(&mut state.waiter, incoming.take()))
            }
        };
        drop(old);
        drop(incoming);
        if done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
struct Finish {
    node: Option<Arc<SequenceNode>>,
}
impl Finish {
    fn complete(&mut self) -> Option<Box<dyn Any + Send + 'static>> {
        let mut node = self.node.take()?;
        let mut own_finish = true;
        let mut first_panic = None;
        loop {
            let (wake, next, previous) = {
                let mut state = node.state.lock().unwrap_or_else(|error| error.into_inner());
                if own_finish {
                    state.finished = true;
                } else {
                    state.predecessor_passed = true;
                }
                if state.finished && state.predecessor_passed && !state.passed {
                    state.passed = true;
                    (
                        state.waiter.take(),
                        state.next.as_ref().and_then(Weak::upgrade),
                        state.previous.take(),
                    )
                } else {
                    (None, None, None)
                }
            };
            drop(previous); // completed metadata, outside node/order/application guards
            if let Some(wake) = wake {
                if let Err(error) = catch_unwind(AssertUnwindSafe(|| wake.wake())) {
                    if first_panic.is_none() {
                        first_panic = Some(error);
                    }
                }
            }
            let Some(next) = next else {
                break;
            };
            node = next;
            own_finish = false;
        }
        first_panic
    }
}
impl Drop for Finish {
    fn drop(&mut self) {
        // Cancellation cannot discard another completed outbox through a wake panic.
        // There is no completion result on explicit owner Drop; secondary panic data
        // is destroyed here off guards. A normal finish instead returns it in the turn.
        drop(self.complete());
    }
}
