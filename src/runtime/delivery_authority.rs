//! Shared short authority leaf for recorded completion and owner metadata.
//! No backend, native lifetime, account policy or payload is owned by this primitive.
use std::sync::{Mutex, MutexGuard, TryLockError};

pub struct DeliveryAuthority {
    mutex: Mutex<()>,
}
pub struct DeliveryLease<'a> {
    _guard: MutexGuard<'a, ()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryLeaseError {
    Busy,
    Poisoned,
}

impl DeliveryAuthority {
    pub fn new() -> Self {
        Self {
            mutex: Mutex::new(()),
        }
    }
    /// Lock order: Runtime -> Model -> authority -> Owner metadata, or worker lane
    /// -> authority -> Owner metadata. Never acquire a worker lane/backend/callback
    /// from authority. Keep the returned lease through the bounded event enqueue.
    /// No await, event consumer invocation, history destruction or reentry here.
    pub fn acquire(&self) -> Result<DeliveryLease<'_>, DeliveryLeaseError> {
        self.mutex
            .lock()
            .map(|guard| DeliveryLease { _guard: guard })
            .map_err(|_| DeliveryLeaseError::Poisoned)
    }
    /// A diagnostic/nonblocking attempt; Busy is not proof that an origin is stale.
    pub fn try_acquire(&self) -> Result<DeliveryLease<'_>, DeliveryLeaseError> {
        self.mutex
            .try_lock()
            .map(|guard| DeliveryLease { _guard: guard })
            .map_err(|error| match error {
                TryLockError::WouldBlock => DeliveryLeaseError::Busy,
                TryLockError::Poisoned(_) => DeliveryLeaseError::Poisoned,
            })
    }
}
impl Default for DeliveryAuthority {
    fn default() -> Self {
        Self::new()
    }
}
