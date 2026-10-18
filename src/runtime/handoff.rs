//! SOURCE PREPARATION ONLY. Off-guard escrow; Runtime wiring is a separate afterimage.
//! Callers must own the receipt/outbox outside transition, Model and authority guards.
//! A receipt may contain a non-Send EnvFuture: it must not be hidden in shared Runtime.
use crate::runtime::EnvFuture;
use futures::Future;
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpawnRefusal {
    Unsupported,
    Stopped,
}

/// Called ONLY after the outer Runtime/Model/authority guards have been released.
/// This receives a gated runner, never the unprotected original future.
/// A recorded adapter must schedule independently runnable work, not a serial
/// consumer which awaits a later ticket before an earlier ticket can be submitted.
pub type TaskSpawner<T> = fn(RecordedTask<T>) -> Result<(), SpawnRefusal>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskPhase {
    Offered,
    Accepted,
    /// The wrapper was polled; it may still be awaiting its predecessor. This is
    /// deliberately NOT proof that storage work started, completed or committed.
    Started,
    Completed,
    Refused,
    CancelledBeforeStart,
    CancelledAfterStart,
    StartedPanicked,
    Recovered,
}

struct State<T> {
    phase: TaskPhase,
    future: Option<EnvFuture<'static, T>>,
    output: Option<T>,
    panic: Option<Box<dyn Any + Send + 'static>>,
    runner_alive: bool,
    waiter: Option<Waker>,
    observer: Option<Waker>,
}

/// Non-Clone ownership of recovery, not an execution or durable-commit ACK.
/// Keep this outside application guards until either recovery or terminal state.
/// Dropping an Accepted receipt does NOT revoke executor-owned work. It loses the
/// observation/recovery owner and is forbidden by the recorded caller contract.
#[must_use = "retain the unpolled recovery owner; acceptance is not completion"]
pub struct HandoffReceipt<T> {
    state: Arc<Mutex<State<T>>>,
    finished: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandoffDisposition {
    Accepted,
    Refused(SpawnRefusal),
    ExecutorPanicked,
}

/// A panic payload is retained, not formatted or logged. A wake panic after
/// acceptance NEVER changes Accepted into Refused and never authorizes replay.
#[must_use = "inspect the disposition and retain the original receipt"]
pub struct HandoffResult<T> {
    pub disposition: HandoffDisposition,
    pub receipt: HandoffReceipt<T>,
    pub panic: Option<Box<dyn Any + Send + 'static>>,
}

#[must_use = "an unstarted future or completed outbox remains caller-owned"]
pub enum TaskOutcome<T> {
    Completed(T),
    Unstarted {
        future: EnvFuture<'static, T>,
        phase: TaskPhase,
    },
    StartedCancelled,
    StartedPanicked(Box<dyn Any + Send + 'static>),
}

/// Non-Clone executor-side capability. Before acceptance it cannot poll the
/// original future, even if an executor eagerly polls or retains it then panics.
pub struct RecordedTask<T> {
    state: Arc<Mutex<State<T>>>,
    running: Option<EnvFuture<'static, T>>,
}

impl<T> HandoffReceipt<T> {
    pub fn phase(&self) -> TaskPhase {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .phase
    }

    /// Recover ONLY the exact unpolled future after refusal or pre-start loss.
    /// There is deliberately no retry, current-scope recapture or clone method.
    pub fn recover(self) -> Result<EnvFuture<'static, T>, Self> {
        let recovered = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(
                state.phase,
                TaskPhase::Refused | TaskPhase::CancelledBeforeStart
            ) {
                let recovered = state.future.take();
                if recovered.is_some() {
                    state.phase = TaskPhase::Recovered;
                }
                recovered
            } else {
                None
            }
        };
        match recovered {
            Some(future) => Ok(future),
            None => Err(self),
        }
    }
}

/// The caller's future remains escrowed through rejection, executor panic and
/// eager inline polling. All arbitrary executor/waker code runs in THIS scope;
/// the caller must therefore invoke this function outside all application guards.
pub fn handoff_recorded<T, F>(spawner: Option<F>, future: EnvFuture<'static, T>) -> HandoffResult<T>
where
    F: FnOnce(RecordedTask<T>) -> Result<(), SpawnRefusal>,
{
    let state = Arc::new(Mutex::new(State {
        phase: TaskPhase::Offered,
        future: Some(future),
        output: None,
        panic: None,
        runner_alive: true,
        waiter: None,
        observer: None,
    }));
    let receipt = HandoffReceipt {
        state: Arc::clone(&state),
        finished: false,
    };
    let task = RecordedTask {
        state: Arc::clone(&state),
        running: None,
    };
    let submitted = match spawner {
        Some(spawn) => catch_unwind(AssertUnwindSafe(|| spawn(task))),
        None => {
            drop(task);
            Ok(Err(SpawnRefusal::Unsupported))
        }
    };
    let mut panic = None;
    let (disposition, waiter) = {
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        let disposition = match submitted {
            Ok(Ok(())) if state.runner_alive => {
                state.phase = TaskPhase::Accepted;
                HandoffDisposition::Accepted
            }
            Ok(Ok(())) => {
                state.phase = TaskPhase::Refused;
                HandoffDisposition::Refused(SpawnRefusal::Stopped)
            }
            Ok(Err(reason)) => {
                state.phase = TaskPhase::Refused;
                HandoffDisposition::Refused(reason)
            }
            Err(error) => {
                // Only a pointer move; destruction of the payload is off-lock.
                panic = Some(error);
                state.phase = TaskPhase::Refused;
                HandoffDisposition::ExecutorPanicked
            }
        };
        (disposition, state.waiter.take())
    };
    if let Some(waiter) = waiter {
        // A retained declined runner may be woken only to finish without polling.
        // A successful runner may now start. Neither path has an escrow lock held.
        if let Err(error) = catch_unwind(AssertUnwindSafe(|| waiter.wake())) {
            if panic.is_none() {
                panic = Some(error);
            }
            // An additional panic payload is destroyed here, outside all our locks.
        }
    }
    HandoffResult {
        disposition,
        receipt,
        panic,
    }
}

impl<T> Future for HandoffReceipt<T> {
    type Output = TaskOutcome<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.finished, "handoff receipt polled after completion");
        let mut incoming = Some(cx.waker().clone());
        let (outcome, old) = {
            let mut state = this.state.lock().unwrap_or_else(|error| error.into_inner());
            let outcome = match state.phase {
                TaskPhase::Completed => state.output.take().map(TaskOutcome::Completed),
                TaskPhase::Refused | TaskPhase::CancelledBeforeStart => {
                    let phase = state.phase;
                    state.future.take().map(|future| {
                        state.phase = TaskPhase::Recovered;
                        TaskOutcome::Unstarted { future, phase }
                    })
                }
                TaskPhase::CancelledAfterStart => Some(TaskOutcome::StartedCancelled),
                TaskPhase::StartedPanicked => state.panic.take().map(TaskOutcome::StartedPanicked),
                _ => None,
            };
            let old = if outcome.is_some() {
                state.observer.take()
            } else {
                std::mem::replace(&mut state.observer, incoming.take())
            };
            (outcome, old)
        };
        drop(old);
        drop(incoming);
        match outcome {
            Some(outcome) => {
                this.finished = true;
                Poll::Ready(outcome)
            }
            None => Poll::Pending,
        }
    }
}

/// Own the terminal wake while the already-completed future is destroyed.
/// Construction and Drop are outside the escrow/application guards. A single
/// cleanup panic still unwinds through this guard and notifies the receipt;
/// that panic continues to the executor boundary, and Completed never changes
/// into retry authority. A second panic from wake during unwind remains outside
/// this contract, as do process aborts. This guard deliberately does not catch it.
struct TerminalNotification(Option<Waker>);
impl Drop for TerminalNotification {
    fn drop(&mut self) {
        if let Some(observer) = self.0.take() {
            observer.wake();
        }
    }
}

impl<T> Future for RecordedTask<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.running.is_none() {
            // Clone can invoke user RawWaker code. It precedes the escrow lock.
            let mut incoming = Some(cx.waker().clone());
            let (ready, replaced) = {
                let mut state = this.state.lock().unwrap_or_else(|error| error.into_inner());
                match state.phase {
                    TaskPhase::Offered => {
                        let replaced = std::mem::replace(&mut state.waiter, incoming.take());
                        (false, replaced)
                    }
                    TaskPhase::Accepted => {
                        this.running = state.future.take();
                        state.phase = TaskPhase::Started;
                        (true, state.waiter.take())
                    }
                    _ => (true, state.waiter.take()),
                }
            };
            drop(replaced);
            drop(incoming);
            if !ready {
                return Poll::Pending;
            }
        }
        let Some(future) = this.running.as_mut() else {
            return Poll::Ready(());
        };
        // No escrow/application guard is held while polling arbitrary work.
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            result => {
                let completed = this.running.take();
                let observer = {
                    let mut state = this.state.lock().unwrap_or_else(|error| error.into_inner());
                    match result {
                        Ok(Poll::Ready(output)) => {
                            state.output = Some(output);
                            state.phase = TaskPhase::Completed;
                        }
                        Err(error) => {
                            state.panic = Some(error);
                            state.phase = TaskPhase::StartedPanicked;
                        }
                        Ok(Poll::Pending) => unreachable!(),
                    }
                    state.observer.take()
                };
                let notification = TerminalNotification(observer);
                drop(completed);
                drop(notification);
                Poll::Ready(())
            }
        }
    }
}

impl<T> Drop for RecordedTask<T> {
    fn drop(&mut self) {
        let (waiter, observer) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.runner_alive = false;
            state.phase = match state.phase {
                TaskPhase::Accepted => TaskPhase::CancelledBeforeStart,
                TaskPhase::Started => TaskPhase::CancelledAfterStart,
                phase => phase,
            };
            // An unpolled future remains with its original receipt, not the runner.
            (state.waiter.take(), state.observer.take())
        };
        drop(waiter);
        if let Some(observer) = observer {
            observer.wake();
        }
        // `running` and the last state Arc are destroyed after the metadata guard.
        // Caller-owned locks and double-panicking destructors are not made safe here.
    }
}

#[cfg(test)]
#[path = "handoff_tests.rs"]
mod tests;
