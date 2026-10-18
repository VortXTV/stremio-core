//! SAVED/UNRUN safe queue fixtures. No custom unsafe RawWaker vtable is introduced.
use super::*;
use futures::{FutureExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;

struct Audit {
    transition: Arc<Mutex<()>>,
    queue: std::sync::Weak<Shared<usize>>,
    calls: AtomicUsize,
    panic: bool,
}
impl Wake for Audit {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            !matches!(
                self.transition.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ),
            "wake under transition"
        );
        if let Some(shared) = self.queue.upgrade() {
            assert!(
                !matches!(
                    shared.queue.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ),
                "wake under queue"
            );
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "synthetic wake panic");
    }
}
fn register(rx: &mut RecordedReceiver<usize>, audit: Arc<Audit>) {
    let waker = Waker::from(audit);
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(rx).poll_next(&mut cx).is_pending());
}
fn audit(tx: &RecordedSender<usize>, transition: &Arc<Mutex<()>>, panic: bool) -> Arc<Audit> {
    Arc::new(Audit {
        transition: Arc::clone(transition),
        queue: Arc::downgrade(&tx.producer.shared),
        calls: AtomicUsize::new(0),
        panic,
    })
}

#[test]
fn recorded_queue_q01_commit_before_close_drains_close_before_commit_refuses() {
    let transition = Mutex::new(());
    let (tx, mut rx) = channel();
    let mut first = Prepared::new(1usize);
    {
        let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
        assert_eq!(tx.commit(&mut first), Ok(()));
    }
    rx.close();
    let mut second = Prepared::new(2);
    {
        let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
        assert_eq!(tx.commit(&mut second), Err(CommitError::Closed));
    }
    assert_eq!(rx.next().now_or_never(), Some(Some(1)));
    assert_eq!(rx.next().now_or_never(), Some(None));
    assert!(rx.is_terminated());
    assert_eq!(rx.next().now_or_never(), Some(None));
}

#[test]
fn recorded_queue_q02_real_wake_on_normal_and_unwind_is_off_transition() {
    for unwind in [false, true] {
        let transition = Arc::new(Mutex::new(()));
        let (tx, mut rx) = channel();
        let audit = audit(&tx, &transition, false);
        register(&mut rx, Arc::clone(&audit));
        let mut item = Prepared::new(7);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
            assert_eq!(tx.commit(&mut item), Ok(()));
            assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
            assert!(!unwind, "synthetic outer unwind");
        }));
        assert_eq!(result.is_err(), unwind);
        assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
        assert_eq!(rx.next().now_or_never(), Some(Some(7)));
        assert_eq!(tx.commit(&mut item), Err(CommitError::Consumed));
        assert!(rx.next().now_or_never().is_none());
    }
}

#[test]
fn recorded_queue_q03_wake_panic_does_not_replay_or_poison_transition() {
    let transition = Arc::new(Mutex::new(()));
    let (tx, mut rx) = channel();
    let audit = audit(&tx, &transition, true);
    register(&mut rx, Arc::clone(&audit));
    let mut item = Prepared::new(9);
    assert!(catch_unwind(AssertUnwindSafe(|| {
        let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
        assert_eq!(tx.commit(&mut item), Ok(()));
    }))
    .is_err());
    assert!(transition.try_lock().is_ok());
    assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
    assert_eq!(rx.next().now_or_never(), Some(Some(9)));
    assert!(rx.next().now_or_never().is_none());
}

#[test]
fn recorded_queue_q04_last_producer_wakes_and_preserves_committed_drain() {
    for queued in [false, true] {
        let transition = Arc::new(Mutex::new(()));
        let (tx, mut rx) = channel();
        let clone = tx.clone();
        let audit = audit(&tx, &transition, false);
        register(&mut rx, Arc::clone(&audit));
        if queued {
            let mut item = Prepared::new(4);
            {
                let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
                assert_eq!(tx.commit(&mut item), Ok(()));
            }
        }
        drop(tx);
        if !queued {
            assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
        }
        drop(clone);
        assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
        if queued {
            assert_eq!(rx.next().now_or_never(), Some(Some(4)));
        }
        assert_eq!(rx.next().now_or_never(), Some(None));
    }
}

#[test]
fn recorded_queue_q05_registration_before_and_after_commit_never_loses_item() {
    for register_first in [false, true] {
        let transition = Arc::new(Mutex::new(()));
        let (tx, mut rx) = channel();
        let audit = audit(&tx, &transition, false);
        if register_first {
            register(&mut rx, Arc::clone(&audit));
        }
        let mut item = Prepared::new(5);
        {
            let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
            assert_eq!(tx.commit(&mut item), Ok(()));
        }
        let waker = Waker::from(Arc::clone(&audit));
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut rx).poll_next(&mut cx),
            Poll::Ready(Some(5))
        ));
        assert_eq!(
            audit.calls.load(Ordering::SeqCst),
            usize::from(register_first)
        );
    }
}

#[test]
fn recorded_queue_q06_exhaustion_closes_without_consuming_or_replaying() {
    let transition = Mutex::new(());
    let (tx, mut rx) = channel();
    tx.producer.shared.queue.lock().unwrap().next = u64::MAX;
    let mut item = Prepared::new(6usize);
    {
        let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
        assert_eq!(tx.commit(&mut item), Err(CommitError::Exhausted));
    }
    assert_eq!(rx.next().now_or_never(), Some(None));
    assert_eq!(tx.producer.shared.queue.lock().unwrap().count, 0);
}

#[test]
fn recorded_queue_q07_safe_waker_final_owner_drop_is_outside_queue() {
    struct DropAudit {
        queue: std::sync::Weak<Shared<usize>>,
        drops: Arc<AtomicUsize>,
    }
    impl Wake for DropAudit {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for DropAudit {
        fn drop(&mut self) {
            assert!(self.queue.upgrade().unwrap().queue.try_lock().is_ok());
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (tx, mut rx) = channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let owned = Arc::new(DropAudit {
        queue: Arc::downgrade(&tx.producer.shared),
        drops: Arc::clone(&drops),
    });
    let waker = Waker::from(Arc::clone(&owned));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut rx).poll_next(&mut cx).is_pending());
    assert_eq!(
        Arc::strong_count(&owned),
        3,
        "poll stores an owned Waker clone"
    );
    drop(cx);
    drop(waker);
    drop(owned);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    rx.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    // This exercises the safe library RawWaker's clone ownership and final Drop,
    // NOT an adversarial custom clone callback (which needs an unsafe vtable).
}

#[test]
fn recorded_queue_q08_rejected_and_receiver_dropped_payloads_die_off_locks() {
    struct Payload {
        transition: Arc<Mutex<()>>,
        seen: Arc<AtomicUsize>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            assert!(!matches!(
                self.transition.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            self.seen.fetch_add(1, Ordering::SeqCst);
        }
    }
    let transition = Arc::new(Mutex::new(()));
    let seen = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = channel();
    let mut first = Prepared::new(Payload {
        transition: Arc::clone(&transition),
        seen: Arc::clone(&seen),
    });
    {
        let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
        assert_eq!(tx.commit(&mut first), Ok(()));
    }
    rx.close();
    let mut refused = Prepared::new(Payload {
        transition: Arc::clone(&transition),
        seen: Arc::clone(&seen),
    });
    {
        let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
        assert_eq!(tx.commit(&mut refused), Err(CommitError::Closed));
        assert_eq!(seen.load(Ordering::SeqCst), 0);
    }
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    drop(rx);
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

#[test]
fn recorded_queue_q09_poison_denies_commit_and_wakes_terminal_receiver_off_guard() {
    let transition = Arc::new(Mutex::new(()));
    let (tx, mut rx) = channel();
    let audit = audit(&tx, &transition, false);
    register(&mut rx, Arc::clone(&audit));
    assert!(catch_unwind(AssertUnwindSafe(|| {
        let _queue = tx.producer.shared.queue.lock().unwrap();
        panic!("synthetic queue poison");
    }))
    .is_err());
    let mut item = Prepared::new(10);
    {
        let _guard = RecordedTransition::new(transition.lock().unwrap(), Some(&tx));
        assert_eq!(tx.commit(&mut item), Err(CommitError::Poisoned));
    }
    assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
    assert_eq!(rx.next().now_or_never(), Some(None));
    assert!(transition.try_lock().is_ok());
}
