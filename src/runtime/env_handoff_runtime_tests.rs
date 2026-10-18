//! SAVED / UNRUN. Actual Runtime + actual Ctx storage producer; synthetic executor
//! dispositions and the existing synthetic D1 authority seam, NOT private Owner proof.
use super::*;
use crate::runtime::{HandoffDisposition, RecordedSubmission};
use std::task::Context;

#[derive(Clone, Copy)]
enum Mode {
    Accept,
    Refuse,
    RetainThenPanic,
}
struct Audit {
    transition: std::sync::Weak<Mutex<()>>,
    model: std::sync::Weak<RwLock<Harness>>,
    authority: Arc<DeliveryAuthority>,
}
thread_local! {
    static MODE: Cell<Mode> = Cell::new(Mode::Accept);
    static AUDIT: RefCell<Option<Audit>> = RefCell::new(None);
    static OFFERS: Cell<usize> = Cell::new(0);
}
pub(super) fn reset_controls() {
    MODE.with(|mode| mode.set(Mode::Accept));
    AUDIT.with(|audit| *audit.borrow_mut() = None);
    OFFERS.with(|offers| offers.set(0));
}
fn watch(runtime: &TestRuntime) {
    let authority = Arc::clone(&runtime.model.read().unwrap().authority);
    AUDIT.with(|audit| {
        *audit.borrow_mut() = Some(Audit {
            transition: Arc::downgrade(&runtime.transition),
            model: Arc::downgrade(&runtime.model),
            authority,
        })
    });
}
pub(super) fn spawn(
    kind: RecordedWorkKind,
    task: RecordedTask<RecordedTurn>,
) -> Result<(), SpawnRefusal> {
    OFFERS.with(|offers| offers.set(offers.get() + 1));
    AUDIT.with(|audit| {
        if let Some(audit) = audit.borrow().as_ref() {
            assert!(
                audit.transition.upgrade().unwrap().try_lock().is_ok(),
                "executor under transition"
            );
            assert!(
                audit.model.upgrade().unwrap().try_write().is_ok(),
                "executor under Model"
            );
            assert!(
                audit.authority.try_acquire().is_ok(),
                "executor under authority"
            );
        }
    });
    if matches!(MODE.with(Cell::get), Mode::Refuse) {
        return Err(SpawnRefusal::Stopped);
    }
    match kind {
        RecordedWorkKind::Sequential => {
            SEQUENTIAL.with(|queue| queue.borrow_mut().push_back(task.boxed_env()))
        }
        _ => CONCURRENT.with(|queue| queue.borrow_mut().push_back(task.boxed_env())),
    }
    if matches!(MODE.with(Cell::get), Mode::RetainThenPanic) {
        panic!("synthetic executor retained runner then panicked");
    }
    Ok(())
}
fn planned(runtime: &TestRuntime, original: &ScopeToken) -> RecordedTurn {
    runtime.handle_recorded_effect_output(
        Msg::Internal(Internal::ProfileChanged),
        EffectScope::recorded(original.clone()),
        AddonEffectContext {
            epoch: 0,
            fenced: true,
        },
    )
}
fn submitted(operation: crate::runtime::RecordedOperation) -> RecordedSubmission {
    operation
        .handoff()
        .unwrap_or_else(|_| panic!("checked ordinal unexpectedly exhausted"))
}
fn completed(receipt: HandoffReceipt<RecordedTurn>) -> RecordedTurn {
    match futures::executor::block_on(receipt) {
        TaskOutcome::Completed(turn) => turn,
        _ => panic!("task completion expected"),
    }
}

#[test]
fn env_handoff_r01_actual_planning_never_calls_executor_and_caller_handoff_is_offguard() {
    reset();
    let (runtime, mut rx, _, original) = new_runtime(false);
    watch(&runtime);
    let turn = planned(&runtime, &original);
    assert_eq!(OFFERS.with(Cell::get), 0);
    assert_eq!(WRITES.with(Cell::get), 0);
    assert_eq!(core_events(&mut rx).len(), 2); // unrelated actual model sentinels
    handoff_turn(turn);
    assert_eq!(OFFERS.with(Cell::get), 2); // prefix + ordinary sequential sibling
    sequential_one();
    sequential_one();
    assert_eq!(WRITES.with(Cell::get), 1);
    assert_eq!(SIBLINGS.with(Cell::get), 1);
    assert_eq!(
        core_events(&mut rx)
            .iter()
            .filter(|event| storage_event(event))
            .count(),
        1
    );
    reset();
}

#[test]
fn env_handoff_r02_refused_original_stays_owned_and_later_offered_sibling_cannot_overtake() {
    reset();
    let (runtime, mut rx, _, original) = new_runtime(false);
    watch(&runtime);
    let (mut operations, panics) = planned(&runtime, &original).into_parts();
    assert!(panics.is_empty());
    assert_eq!(operations.len(), 2);
    MODE.with(|mode| mode.set(Mode::Refuse));
    let prefix = submitted(operations.remove(0));
    assert_eq!(
        prefix.result.disposition,
        HandoffDisposition::Refused(SpawnRefusal::Stopped)
    );
    let original = prefix
        .result
        .receipt
        .recover()
        .unwrap_or_else(|_| panic!("original unpolled prefix"));
    MODE.with(|mode| mode.set(Mode::Accept));
    let sibling = submitted(operations.remove(0));
    let mut task = SEQUENTIAL
        .with(|queue| queue.borrow_mut().pop_front())
        .unwrap();
    assert!(task
        .as_mut()
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
        .is_pending());
    assert_eq!(WRITES.with(Cell::get), 0);
    assert_eq!(SIBLINGS.with(Cell::get), 0);
    assert_eq!(
        core_events(&mut rx)
            .iter()
            .filter(|event| storage_event(event))
            .count(),
        0
    );
    // Explicit fixture-only drive of the returned original operation. Production
    // neither retries nor recreates a root, ticket, auth attempt or storage capture.
    handoff_turn(futures::executor::block_on(original));
    futures::executor::block_on(task);
    handoff_turn(completed(sibling.result.receipt));
    assert_eq!(WRITES.with(Cell::get), 1);
    assert_eq!(SIBLINGS.with(Cell::get), 1);
    assert_eq!(
        core_events(&mut rx)
            .iter()
            .filter(|event| storage_event(event))
            .count(),
        1
    );
    reset();
}

#[test]
fn env_handoff_r03_executor_panic_recovers_actual_prefix_and_revokes_retained_runner() {
    reset();
    let (runtime, mut rx, _, original) = new_runtime(false);
    watch(&runtime);
    let (mut operations, panics) = planned(&runtime, &original).into_parts();
    assert!(panics.is_empty());
    MODE.with(|mode| mode.set(Mode::RetainThenPanic));
    let prefix = submitted(operations.remove(0));
    assert_eq!(
        prefix.result.disposition,
        HandoffDisposition::ExecutorPanicked
    );
    assert!(prefix.result.panic.is_some());
    let original = prefix
        .result
        .receipt
        .recover()
        .unwrap_or_else(|_| panic!("panic retained original"));
    sequential_one(); // retained revoked runner cannot execute the actual producer
    assert_eq!(WRITES.with(Cell::get), 0);
    assert_eq!(
        core_events(&mut rx)
            .iter()
            .filter(|event| storage_event(event))
            .count(),
        0
    );
    assert!(runtime.transition.try_lock().is_ok());
    assert!(runtime.model.try_write().is_ok());
    drop(original);
    drop(operations); // explicit off-guard cancellation, no ACK/retry
    reset();
}

#[test]
fn env_handoff_r04_pending_observer_refusal_does_not_park_fifo_or_refresh_original_origin() {
    for cancel in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(true);
        watch(&runtime);
        let (mut operations, panics) = planned(&runtime, &original).into_parts();
        assert!(panics.is_empty());
        let prefix = submitted(operations.remove(0));
        let sibling = submitted(operations.remove(0));
        sequential_one(); // receipt is retained locally, not in helper supervisor
        let (mut observers, panics) = completed(prefix.result.receipt).into_parts();
        assert!(panics.is_empty());
        assert_eq!(observers.len(), 1);
        assert_eq!(observers[0].kind(), RecordedWorkKind::CompletionObserver);
        MODE.with(|mode| mode.set(Mode::Refuse));
        let observer = submitted(observers.remove(0));
        assert_eq!(
            observer.result.disposition,
            HandoffDisposition::Refused(SpawnRefusal::Stopped)
        );
        let original_observer = observer
            .result
            .receipt
            .recover()
            .unwrap_or_else(|_| panic!("original observer"));
        sequential_one(); // sibling finishes without Unknown resolution or observer acceptance
        handoff_turn(completed(sibling.result.receipt));
        assert_eq!(WRITES.with(Cell::get), 1);
        assert_eq!(SIBLINGS.with(Cell::get), 1);
        let captures = runtime
            .model
            .read()
            .unwrap()
            .root_captures
            .load(Ordering::SeqCst);
        if cancel {
            runtime.cancel_pending_completions();
        }
        let (publisher, events) = pending.lock().unwrap().take().unwrap();
        assert_eq!(publisher.submit(events), CompletionSubmission::Queued);
        // Explicit fixture-only observer drive. Select retains the v3 cancellation
        // semantics; no new origin is minted and no durable operation is replayed.
        handoff_turn(futures::executor::block_on(original_observer));
        assert_eq!(
            runtime
                .model
                .read()
                .unwrap()
                .root_captures
                .load(Ordering::SeqCst),
            captures
        );
        assert_eq!(
            core_events(&mut rx)
                .iter()
                .filter(|event| storage_event(event))
                .count(),
            usize::from(!cancel)
        );
        reset();
    }
}

#[test]
fn env_handoff_r05_turn_panic_returns_already_collected_prefix_and_siblings() {
    reset();
    let (runtime, mut rx, _, original) = new_runtime(false);
    let turn = runtime.capture_recorded_turn(|work| {
        runtime.handle_effect_output_scoped_with_work(
            Msg::Internal(Internal::ProfileChanged),
            EffectScope::recorded(original),
            AddonEffectContext {
                epoch: 0,
                fenced: true,
            },
            work,
        );
        panic!("after already-collected prefix and sibling");
    });
    let (operations, panics) = turn.into_parts();
    assert_eq!(operations.len(), 2);
    assert_eq!(panics.len(), 1);
    assert_eq!(OFFERS.with(Cell::get), 0);
    assert_eq!(WRITES.with(Cell::get), 0);
    assert!(!matches!(
        runtime.transition.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
    assert_eq!(
        core_events(&mut rx)
            .iter()
            .filter(|event| storage_event(event))
            .count(),
        0
    );
    // Transition poisoning is intentionally visible, not "repaired" by this task.
    drop(operations);
    drop(panics); // owner explicitly cancels outside all guards
    reset();
}

#[test]
fn env_handoff_r06_cancelling_middle_ticket_cannot_release_tail_before_original_prefix() {
    reset();
    let (runtime, mut rx, _, original) = new_runtime(false);
    let mut turn = planned(&runtime, &original);
    let tail_runtime = runtime.clone();
    turn.push(
        runtime.recorded_executor.as_ref().unwrap(),
        RecordedWorkKind::Sequential,
        async move {
            tail_runtime.handle_recorded_effect_output(
                Msg::Event(Event::NotificationsDismissed {
                    id: "tail-after-cancelled-middle".into(),
                }),
                EffectScope::recorded(original),
                AddonEffectContext {
                    epoch: 0,
                    fenced: false,
                },
            )
        }
        .boxed_env(),
    );
    let (mut operations, panics) = turn.into_parts();
    assert!(panics.is_empty());
    assert_eq!(operations.len(), 3);
    let prefix = operations.remove(0);
    drop(operations.remove(0)); // explicit cancellation of only the middle position
    let tail = submitted(operations.remove(0));
    let mut tail_task = SEQUENTIAL
        .with(|queue| queue.borrow_mut().pop_front())
        .unwrap();
    assert!(
        tail_task
            .as_mut()
            .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
            .is_pending(),
        "tail overtook unfinished original prefix"
    );
    assert_eq!(WRITES.with(Cell::get), 0);
    assert!(!core_events(&mut rx).iter().any(|event| matches!(event,
        Event::NotificationsDismissed { id } if id == "tail-after-cancelled-middle")));
    let prefix = submitted(prefix);
    sequential_one();
    handoff_turn(completed(prefix.result.receipt));
    futures::executor::block_on(tail_task);
    handoff_turn(completed(tail.result.receipt));
    assert_eq!(WRITES.with(Cell::get), 1);
    assert_eq!(SIBLINGS.with(Cell::get), 0); // the middle was deliberately cancelled
    let events = core_events(&mut rx);
    assert!(storage_event(&events[0]));
    assert!(
        matches!(events.last(), Some(Event::NotificationsDismissed { id }) if id == "tail-after-cancelled-middle")
    );
    reset();
}
