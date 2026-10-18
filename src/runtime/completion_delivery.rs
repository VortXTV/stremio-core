//! Source-only D1 v3: original-authority commit into the recorded receiver's queue.
//! Private Owner/Ctx authority issuance and exact Unknown publisher binding are NOT here.
use super::*;
use crate::runtime::{CompletionEvents, PrefixCompletion, ScopeToken};
use std::sync::{atomic::AtomicBool, Weak};

pub(super) struct CompletionLifetime {
    cancel_on_drop: Mutex<Option<futures::channel::oneshot::Sender<()>>>,
    active: AtomicBool,
    cancelled: futures::future::Shared<futures::channel::oneshot::Receiver<()>>,
    #[cfg(test)]
    pub(super) send_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl CompletionLifetime {
    pub(super) fn new() -> Self {
        let (sender, receiver) = futures::channel::oneshot::channel();
        Self {
            cancel_on_drop: Mutex::new(Some(sender)),
            active: AtomicBool::new(true),
            cancelled: receiver.shared(),
            #[cfg(test)]
            send_probe: Mutex::new(None),
        }
    }
    fn cancel(&self) -> Option<futures::channel::oneshot::Sender<()>> {
        self.active.store(false, Ordering::SeqCst);
        self.cancel_on_drop
            .lock()
            .expect("completion cancellation lock failed")
            .take()
    }
}

struct CompletionTarget<E: Env, M: Model<E>> {
    model: Weak<RwLock<M>>,
    tx: RecordedSender<RuntimeEvent<E, M>>,
    addon_epoch: Weak<AtomicU64>,
    transition: Weak<Mutex<()>>,
    lifetime: Weak<CompletionLifetime>,
}
impl<E: Env, M: Model<E>> CompletionTarget<E, M> {
    fn deliver(self, events: CompletionEvents, original: ScopeToken, addon: AddonEffectContext) {
        // Input ownership, prepared list nodes, last upgraded model/lifetime Arcs,
        // and rejected output ALL outlive the inner guards, including on unwind.
        let mut prepared =
            events.map(|slot| slot.map(|event| Prepared::new(RuntimeEvent::CoreEvent(event))));
        let Some(transition) = self.transition.upgrade() else {
            return;
        };
        let live_owner;
        let model_owner;
        let epoch_owner;
        let mut failed = false;
        {
            let _transition = RecordedTransition::new(
                transition.lock().expect("transition lock failed"),
                Some(&self.tx),
            );
            live_owner = self.lifetime.upgrade();
            let Some(live) = live_owner.as_ref() else {
                return;
            };
            if !live.active.load(Ordering::SeqCst) {
                return;
            }
            model_owner = self.model.upgrade();
            let Some(model) = model_owner.as_ref() else {
                return;
            };
            epoch_owner = self.addon_epoch.upgrade();
            let Some(epoch) = epoch_owner.as_ref() else {
                return;
            };
            // Borrow slots: an owning iterator made here would drop unsent history
            // under transition when the next origin/addon validation refuses.
            for slot in &mut prepared {
                let Some(output) = slot.as_mut() else {
                    continue;
                };
                let model_guard = model.read().expect("model read failed");
                let lease = match model_guard.completion_lease(&original) {
                    Ok(lease) => lease,
                    Err(_) => return,
                };
                if addon.fenced && addon.epoch != epoch.load(Ordering::SeqCst) {
                    return;
                }
                #[cfg(test)]
                {
                    let probe = live
                        .send_probe
                        .lock()
                        .expect("completion probe lock failed")
                        .clone();
                    if let Some(probe) = probe {
                        probe();
                    }
                }
                // Commit checks receiver-close under the SAME queue lock as close,
                // while ORIGINAL authority is retained. It invokes no Waker/Drop.
                // Its taken Waker/failed payload is owned by the outer transition.
                failed = self.tx.commit(output).is_err();
                drop(lease);
                drop(model_guard);
                if failed {
                    break;
                }
            }
        }
        // No authority/Model/transition is held through wake or failure panic.
        // An already committed node is never retried, even if its later wake panics.
        if failed {
            panic!("emit completion event failed");
        }
    }
}

impl<E, M> Runtime<E, M>
where
    E: Env + Send + 'static,
    M: Model<E> + Send + Sync + 'static,
{
    /// Terminal observation cancellation; never durable rollback or host quiescence.
    pub fn cancel_pending_completions(&self) {
        let Some(live) = self.completion_lifetime.as_ref() else {
            return;
        };
        let cancelled_sender;
        {
            let _transition = self.lock_transition();
            cancelled_sender = live.cancel();
        }
        drop(cancelled_sender); // oneshot wake AFTER transition, never in cancel().
    }

    pub(super) fn enqueue_prefix(
        &self,
        prefix: crate::runtime::LocalPrefix,
        addon: AddonEffectContext,
    ) {
        // Only scoped recorded plans can carry LocalPrefix. Legacy new accepts Effect,
        // never this plan type; no ordinary Legacy send/callback path changes here.
        let (Some(tx), Some(live)) = (self.tx.recorded(), self.completion_lifetime.as_ref()) else {
            return;
        };
        let target = CompletionTarget {
            model: Arc::downgrade(&self.model),
            tx: tx.clone(),
            addon_epoch: Arc::downgrade(&self.addon_epoch),
            transition: Arc::downgrade(&self.transition),
            lifetime: Arc::downgrade(live),
        };
        let cancelled = live.cancelled.clone();
        let original = prefix.origin;
        // Existing consuming Env handoff remains a separately recorded SOURCE gap:
        // rejecting/panicking implementations can destroy this future under transition.
        E::exec_sequential(prefix.future.then(move |output| async move {
            match output {
                PrefixCompletion::Final(events) => target.deliver(events, original, addon),
                PrefixCompletion::Pending(pending) => {
                    // Move the ORIGINAL one-shot receiver/target/token/addon, once.
                    // Sequential FIFO returns before pending resolution is awaited.
                    E::exec_concurrent(async move {
                        if let futures::future::Either::Left((Some(events), _)) =
                            futures::future::select(Box::pin(pending.receive()), cancelled).await
                        {
                            target.deliver(events, original, addon);
                        }
                    });
                }
            }
        }));
    }
}
