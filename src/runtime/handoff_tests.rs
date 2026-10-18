//! Saved source controls only; UNRUN. No unsafe/custom RawWaker vtable.
use super::*;
use crate::runtime::EnvFutureExt;
use futures::{future, FutureExt};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;

thread_local! {
    static TASKS: RefCell<VecDeque<RecordedTask<usize>>> = RefCell::new(VecDeque::new());
    static POLLS: Cell<usize> = Cell::new(0);
    static WATCH_STATE: RefCell<Option<std::sync::Weak<Mutex<State<usize>>>>> = RefCell::new(None);
}
fn reset() {
    WATCH_STATE.with(|state| *state.borrow_mut() = None);
    TASKS.with(|tasks| tasks.borrow_mut().clear());
    POLLS.with(|polls| polls.set(0));
}
fn input() -> EnvFuture<'static, usize> {
    future::lazy(|_| {
        POLLS.with(|polls| polls.set(polls.get() + 1));
        7
    })
    .boxed_env()
}
fn accept(task: RecordedTask<usize>) -> Result<(), SpawnRefusal> {
    TASKS.with(|tasks| tasks.borrow_mut().push_back(task));
    Ok(())
}
fn retain_then_refuse(task: RecordedTask<usize>) -> Result<(), SpawnRefusal> {
    accept(task)?;
    Err(SpawnRefusal::Stopped)
}
fn eager_then_panic(mut task: RecordedTask<usize>) -> Result<(), SpawnRefusal> {
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(Pin::new(&mut task).poll(&mut cx).is_pending());
    assert_eq!(POLLS.with(Cell::get), 0);
    accept(task)?;
    panic!("executor retained runner then panicked");
}
fn one() {
    let task = TASKS.with(|tasks| tasks.borrow_mut().pop_front()).unwrap();
    futures::executor::block_on(task);
}

#[test]
fn handoff_h01_default_decline_returns_exact_unpolled_future() {
    reset();
    let result = handoff_recorded(None::<TaskSpawner<usize>>, input());
    assert_eq!(
        result.disposition,
        HandoffDisposition::Refused(SpawnRefusal::Unsupported)
    );
    assert_eq!(POLLS.with(Cell::get), 0);
    let future = result
        .receipt
        .recover()
        .unwrap_or_else(|_| panic!("exact unpolled owner"));
    // Explicit fixture-only drive of the returned original, NOT production retry.
    assert_eq!(futures::executor::block_on(future), 7);
    assert_eq!(POLLS.with(Cell::get), 1);
}

#[test]
fn handoff_h02_retained_declined_runner_cannot_steal_recovered_future() {
    reset();
    let result = handoff_recorded(Some(retain_then_refuse), input());
    let future = result
        .receipt
        .recover()
        .unwrap_or_else(|_| panic!("recovery"));
    one(); // a misbehaving executor still owns a revoked runner
    assert_eq!(POLLS.with(Cell::get), 0);
    assert_eq!(futures::executor::block_on(future), 7);
    assert_eq!(POLLS.with(Cell::get), 1);
}

#[test]
fn handoff_h03_eager_poll_then_executor_panic_cannot_poll_original() {
    reset();
    let result = handoff_recorded(Some(eager_then_panic), input());
    assert_eq!(result.disposition, HandoffDisposition::ExecutorPanicked);
    assert!(result.panic.is_some());
    assert_eq!(
        POLLS.with(Cell::get),
        0,
        "original was polled before executor acceptance"
    );
    let future = result
        .receipt
        .recover()
        .unwrap_or_else(|_| panic!("panic recovery"));
    one();
    assert_eq!(POLLS.with(Cell::get), 0);
    drop(future); // explicit terminal cancellation, not a completed storage ACK
}

#[test]
fn handoff_h04_accepted_then_executor_loss_before_poll_is_recoverable() {
    reset();
    let result = handoff_recorded(Some(accept), input());
    assert_eq!(result.disposition, HandoffDisposition::Accepted);
    let receipt = result
        .receipt
        .recover()
        .err()
        .expect("live accepted work cannot be replayed");
    TASKS.with(|tasks| tasks.borrow_mut().clear());
    assert_eq!(receipt.phase(), TaskPhase::CancelledBeforeStart);
    match futures::executor::block_on(receipt) {
        TaskOutcome::Unstarted {
            future,
            phase: TaskPhase::CancelledBeforeStart,
        } => drop(future),
        _ => panic!("original unpolled future required"),
    }
    assert_eq!(POLLS.with(Cell::get), 0);
}

#[test]
fn handoff_h05_started_cancellation_is_never_recovered_as_unpolled() {
    reset();
    let future = future::poll_fn(|_| {
        POLLS.with(|polls| polls.set(polls.get() + 1));
        Poll::Pending::<usize>
    })
    .boxed_env();
    let result = handoff_recorded(Some(accept), future);
    let mut task = TASKS.with(|tasks| tasks.borrow_mut().pop_front()).unwrap();
    assert!(Pin::new(&mut task)
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
        .is_pending());
    drop(task);
    assert!(matches!(
        futures::executor::block_on(result.receipt),
        TaskOutcome::StartedCancelled
    ));
    assert_eq!(POLLS.with(Cell::get), 1);
}

struct Reenter;
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        let mut task = TASKS.with(|tasks| tasks.borrow_mut().pop_front()).unwrap();
        let ready = Pin::new(&mut task)
            .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
            .is_ready();
        if !ready {
            TASKS.with(|tasks| tasks.borrow_mut().push_back(task));
        }
    }
}
fn eager_reenter(mut task: RecordedTask<usize>) -> Result<(), SpawnRefusal> {
    let waker = Waker::from(Arc::new(Reenter));
    assert!(Pin::new(&mut task)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    accept(task)
}
#[test]
fn handoff_h06_acceptance_wake_reentry_has_no_escrow_lock() {
    reset();
    let result = handoff_recorded(Some(eager_reenter), input());
    assert_eq!(result.disposition, HandoffDisposition::Accepted);
    assert!(matches!(
        futures::executor::block_on(result.receipt),
        TaskOutcome::Completed(7)
    ));
    assert_eq!(POLLS.with(Cell::get), 1);
}

struct PanicWake;
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("accepted wake panic");
    }
}
fn eager_panic_wake(mut task: RecordedTask<usize>) -> Result<(), SpawnRefusal> {
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(Pin::new(&mut task)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    accept(task)
}
#[test]
fn handoff_h07_post_acceptance_wake_panic_does_not_authorize_retry() {
    reset();
    let result = handoff_recorded(Some(eager_panic_wake), input());
    assert_eq!(result.disposition, HandoffDisposition::Accepted);
    assert!(result.panic.is_some());
    let receipt = result
        .receipt
        .recover()
        .err()
        .expect("accepted receipt retained");
    one();
    assert!(matches!(
        futures::executor::block_on(receipt),
        TaskOutcome::Completed(7)
    ));
    assert_eq!(POLLS.with(Cell::get), 1);
}

#[test]
fn handoff_h08_started_future_panic_is_a_terminal_owned_outcome() {
    reset();
    let result = handoff_recorded(
        Some(accept),
        future::poll_fn(|_| -> Poll<usize> { panic!("original future panic") }).boxed_env(),
    );
    one();
    assert!(matches!(
        futures::executor::block_on(result.receipt),
        TaskOutcome::StartedPanicked(_)
    ));
}

struct FinalWakerOwner {
    drops: Arc<AtomicUsize>,
}
impl Wake for FinalWakerOwner {
    fn wake(self: Arc<Self>) {}
}
impl Drop for FinalWakerOwner {
    fn drop(&mut self) {
        WATCH_STATE.with(|state| {
            assert!(
                state
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap()
                    .try_lock()
                    .is_ok(),
                "final Waker owner dropped under escrow mutex"
            )
        });
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn handoff_h09_receipt_waiter_replacement_releases_safe_final_owner() {
    reset();
    let result = handoff_recorded(Some(accept), input());
    let mut receipt = result.receipt;
    WATCH_STATE.with(|state| *state.borrow_mut() = Some(Arc::downgrade(&receipt.state)));
    let drops = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(FinalWakerOwner {
        drops: Arc::clone(&drops),
    }));
    assert!(Pin::new(&mut receipt)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(waker);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(Pin::new(&mut receipt)
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
        .is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    one();
    assert!(matches!(
        receipt.now_or_never(),
        Some(TaskOutcome::Completed(7))
    ));
}

// H10/H11 manually poll and synchronously wake on this owning test thread.
// Keep non-Send escrow access in WATCH_STATE, not in the safe Waker owner.
struct TerminalObserver {
    wakes: Arc<AtomicUsize>,
}
impl Wake for TerminalObserver {
    fn wake(self: Arc<Self>) {
        WATCH_STATE.with(|state| {
            let state = state.borrow().as_ref().unwrap().upgrade().unwrap();
            assert!(
                state.try_lock().is_ok(),
                "terminal observer woke under escrow mutex"
            );
        });
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReadyWithCleanup {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}
impl Future for ReadyWithCleanup {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(23)
    }
}
impl Drop for ReadyWithCleanup {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("single original-future cleanup panic");
        }
    }
}

fn terminal_observer_case(panic_on_drop: bool) {
    reset();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let result = handoff_recorded(
        Some(accept),
        ReadyWithCleanup {
            polls: Arc::clone(&polls),
            drops: Arc::clone(&drops),
            panic_on_drop,
        }
        .boxed_env(),
    );
    assert_eq!(result.disposition, HandoffDisposition::Accepted);
    assert!(result.panic.is_none());
    let mut receipt = result.receipt;
    WATCH_STATE.with(|state| *state.borrow_mut() = Some(Arc::downgrade(&receipt.state)));
    let observer = Waker::from(Arc::new(TerminalObserver {
        wakes: Arc::clone(&wakes),
    }));
    // Register BEFORE running the task. A later unsolicited poll must not mask a
    // missing terminal wake; the wake count is asserted before collecting output.
    assert!(Pin::new(&mut receipt)
        .poll(&mut Context::from_waker(&observer))
        .is_pending());
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    let mut task = TASKS.with(|tasks| tasks.borrow_mut().pop_front()).unwrap();
    let worker = catch_unwind(AssertUnwindSafe(|| {
        Pin::new(&mut task).poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
    }));
    if panic_on_drop {
        let panic = worker.expect_err("cleanup panic must propagate to executor");
        assert_eq!(
            panic.downcast_ref::<&'static str>(),
            Some(&"single original-future cleanup panic")
        );
    } else {
        assert!(worker.expect("normal cleanup").is_ready());
    }
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "registered terminal observer was not notified after original-future cleanup"
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(receipt.phase(), TaskPhase::Completed);
    // Even on cleanup panic the body completed: no recovery/retry permission.
    receipt = receipt
        .recover()
        .err()
        .expect("completed work cannot become unstarted");
    drop(task);
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "runner Drop replayed terminal notification"
    );
    assert!(matches!(
        Pin::new(&mut receipt).poll(&mut Context::from_waker(&observer)),
        Poll::Ready(TaskOutcome::Completed(23))
    ));
    assert!(receipt.finished);
    assert!(
        receipt.state.lock().unwrap().output.is_none(),
        "output was not consumed exactly once"
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _ = Pin::new(&mut receipt).poll(&mut Context::from_waker(&observer));
        }))
        .is_err(),
        "completed receipt permitted a second result"
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn handoff_h10_registered_observer_normal_completion_is_exactly_once() {
    terminal_observer_case(false);
}

#[test]
fn handoff_h11_cleanup_panic_notifies_registered_observer_and_preserves_completed_output() {
    terminal_observer_case(true);
}
