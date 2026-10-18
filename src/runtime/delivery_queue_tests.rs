//! SAVED/UNRUN actual Runtime callback tests, nested under delivery_origin_tests.
//! Issuer is the explicitly synthetic Harness; no private Owner/auth closure implied.
use super::*;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Wake, Waker};

struct Callback {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    calls: AtomicUsize,
}
impl Wake for Callback {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let action = self.callback.lock().unwrap().take();
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(action) = action {
            action();
        } // no callback-holder lock here
    }
}
fn register(rx: &mut TestRx, callback: impl FnOnce() + Send + 'static) -> Arc<Callback> {
    let callback = Arc::new(Callback {
        callback: Mutex::new(Some(Box::new(callback))),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&callback));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(rx).poll_next(&mut cx).is_pending());
    callback
}
fn assert_unlocked(runtime: &TestRuntime, authority: &DeliveryAuthority) {
    assert!(
        runtime.transition.try_lock().is_ok(),
        "callback under transition"
    );
    assert!(runtime.model.try_write().is_ok(), "callback under model");
    assert!(authority.try_acquire().is_ok(), "callback under authority");
}
fn complete(deferred: bool, pending: &PendingSlot) {
    if deferred {
        sequential_one();
        let (publisher, events) = pending.lock().unwrap().take().unwrap();
        assert_eq!(publisher.submit(events), CompletionSubmission::Queued);
        concurrent_one();
    } else {
        sequential_one();
    }
}

#[test]
fn delivery_origin_d14_real_receiver_wake_reenters_dispatch_and_preserves_order() {
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(deferred);
        begin(&runtime, &original, true);
        let siblings = core_events(&mut rx);
        assert_eq!(siblings.len(), 2);
        let reentrant = runtime.clone();
        let authority = Arc::clone(&runtime.model.read().unwrap().authority);
        let callback = register(&mut rx, move || {
            assert_unlocked(&reentrant, &authority);
            reentrant.dispatch(RuntimeAction {
                action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![])),
                field: None,
            });
            reentrant.handle_scheduled_effect_output(
                Msg::Event(Event::NotificationsDismissed {
                    id: "reentrant-after-commit".into(),
                }),
                EffectScope::recorded(original),
                AddonEffectContext {
                    epoch: 0,
                    fenced: false,
                },
            );
        });
        complete(deferred, &pending);
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
        let events = core_events(&mut rx);
        assert!(
            storage_event(&events[0]),
            "original completion precedes reentrant root output"
        );
        assert!(
            matches!(events.last(), Some(Event::NotificationsDismissed { id }) if id == "reentrant-after-commit")
        );
        assert_eq!(
            events.iter().filter(|event| storage_event(event)).count(),
            1
        );
        reset();
    }
}

#[test]
fn delivery_origin_d15_wake_closes_and_polls_exact_committed_node_then_cancels() {
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(deferred);
        begin(&runtime, &original, true);
        let _siblings = core_events(&mut rx);
        let holder: Arc<Mutex<Option<TestRx>>> = Arc::new(Mutex::new(None));
        let receiver = Arc::clone(&holder);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&captured);
        let reentrant = runtime.clone();
        let authority = Arc::clone(&runtime.model.read().unwrap().authority);
        let callback = register(&mut rx, move || {
            assert_unlocked(&reentrant, &authority);
            let mut rx = receiver.lock().unwrap().take().unwrap();
            rx.close();
            let events = core_events(&mut rx);
            assert!(matches!(rx.next().now_or_never(), Some(None)));
            *seen.lock().unwrap() = events;
            drop(rx);
            reentrant.cancel_pending_completions();
        });
        *holder.lock().unwrap() = Some(rx);
        complete(deferred, &pending);
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .iter()
                .filter(|event| storage_event(event))
                .count(),
            1
        );
        assert_eq!(WRITES.with(Cell::get), 1);
        reset();
    }
}

#[test]
fn delivery_origin_d16_wake_panic_is_after_release_and_does_not_replay_completion() {
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(deferred);
        begin(&runtime, &original, true);
        let _siblings = core_events(&mut rx);
        let reentrant = runtime.clone();
        let authority = Arc::clone(&runtime.model.read().unwrap().authority);
        let callback = register(&mut rx, move || {
            assert_unlocked(&reentrant, &authority);
            panic!("synthetic actual Runtime receiver wake panic");
        });
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| complete(
                deferred, &pending
            )))
            .is_err()
        );
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
        assert!(runtime.transition.try_lock().is_ok());
        assert!(runtime.model.try_write().is_ok());
        let events = core_events(&mut rx);
        assert_eq!(
            events.iter().filter(|event| storage_event(event)).count(),
            1
        );
        assert!(core_events(&mut rx).is_empty());
        assert_eq!(WRITES.with(Cell::get), 1);
        reset();
    }
}

#[test]
fn delivery_origin_d17_last_runtime_producer_terminates_real_receiver() {
    reset();
    let (runtime, mut rx, _pending, _original) = new_runtime(false);
    let callback = register(&mut rx, || {}); // deliberately does NOT retain a Runtime
    let clone = runtime.clone();
    drop(runtime);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 0);
    drop(clone);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(rx.next().now_or_never(), Some(None)));
    reset();
}

#[test]
fn delivery_origin_d18_safe_waker_last_owner_drop_is_after_all_runtime_guards() {
    struct OwnerDrop {
        transition: std::sync::Weak<Mutex<()>>,
        model: std::sync::Weak<RwLock<Harness>>,
        authority: Arc<DeliveryAuthority>,
        drops: Arc<AtomicUsize>,
    }
    impl Wake for OwnerDrop {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for OwnerDrop {
        fn drop(&mut self) {
            assert!(self.transition.upgrade().unwrap().try_lock().is_ok());
            assert!(self.model.upgrade().unwrap().try_write().is_ok());
            assert!(self.authority.try_acquire().is_ok());
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    for deferred in [false, true] {
        reset();
        let (runtime, mut rx, pending, original) = new_runtime(deferred);
        begin(&runtime, &original, true);
        let _siblings = core_events(&mut rx);
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Arc::new(OwnerDrop {
            transition: Arc::downgrade(&runtime.transition),
            model: Arc::downgrade(&runtime.model),
            authority: Arc::clone(&runtime.model.read().unwrap().authority),
            drops: Arc::clone(&drops),
        });
        let waker = Waker::from(Arc::clone(&owner));
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut rx).poll_next(&mut cx).is_pending());
        assert_eq!(Arc::strong_count(&owner), 3);
        drop(cx);
        drop(waker);
        drop(owner); // only the queue's owned Waker clone remains
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        complete(deferred, &pending);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            core_events(&mut rx)
                .iter()
                .filter(|event| storage_event(event))
                .count(),
            1
        );
        reset();
    }
}

#[test]
fn delivery_origin_d19_last_runtime_cancels_pending_before_last_producer_termination() {
    reset();
    let (runtime, mut rx, pending, original) = new_runtime(true);
    begin(&runtime, &original, true);
    sequential_one(); // original Pending observer installed without parking FIFO
    sequential_one(); // unrelated Runtime-owning future completes
    let _siblings = core_events(&mut rx);
    let callback = register(&mut rx, || {});
    drop(runtime); // drops cancellation sender; observer still owns its original producer
    concurrent_one(); // sees cancellation, drops original observer/producer; never replans
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(rx.next().now_or_never(), Some(None)));
    let (publisher, events) = pending.lock().unwrap().take().unwrap();
    assert_eq!(publisher.submit(events), CompletionSubmission::ReceiverGone);
    assert_eq!(WRITES.with(Cell::get), 1);
    assert_eq!(SIBLINGS.with(Cell::get), 1);
    reset();
}
