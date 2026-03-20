//! Fault injection framework for testing.
//!
//! Enabled only when the `fault-injection` feature is active.
//! In normal builds, all injection points are no-ops with zero overhead.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Global fault injection state.
pub struct FaultInjector {
    /// Whether fault injection is enabled.
    pub enabled: AtomicBool,
    /// Counter: how many injection points have been hit.
    pub hit_count: AtomicU64,
    /// Trigger after this many hits (0 = never trigger).
    pub trigger_after: AtomicU64,
    /// Whether the fault has been triggered.
    pub triggered: AtomicBool,
}

impl FaultInjector {
    pub const fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            hit_count: AtomicU64::new(0),
            trigger_after: AtomicU64::new(0),
            triggered: AtomicBool::new(false),
        }
    }

    /// Enable fault injection, triggering after `after` hits.
    pub fn arm(&self, after: u64) {
        self.hit_count.store(0, Ordering::SeqCst);
        self.trigger_after.store(after, Ordering::SeqCst);
        self.triggered.store(false, Ordering::SeqCst);
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Disable fault injection and reset state.
    pub fn disarm(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        self.triggered.store(false, Ordering::SeqCst);
        self.hit_count.store(0, Ordering::SeqCst);
    }

    /// Check an injection point. Returns true if the fault should trigger.
    pub fn check(&self) -> bool {
        if !self.enabled.load(Ordering::SeqCst) {
            return false;
        }
        let count = self.hit_count.fetch_add(1, Ordering::SeqCst) + 1;
        let trigger = self.trigger_after.load(Ordering::SeqCst);
        if trigger > 0 && count >= trigger && !self.triggered.swap(true, Ordering::SeqCst) {
            return true;
        }
        false
    }

    /// Whether the fault has been triggered at least once.
    pub fn was_triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

/// Global fault injector for WAL write operations.
pub static WAL_WRITE_FAULT: FaultInjector = FaultInjector::new();

/// Global fault injector for checkpoint operations.
pub static CHECKPOINT_FAULT: FaultInjector = FaultInjector::new();

/// Global fault injector for storage write operations.
pub static STORAGE_WRITE_FAULT: FaultInjector = FaultInjector::new();

/// Check a fault injection point. Returns Err if the fault triggers.
///
/// In release builds without the fault-injection feature, this is a no-op.
#[inline]
pub fn check_fault(injector: &FaultInjector, msg: &str) -> Result<(), crate::error::GqliteError> {
    if injector.check() {
        Err(crate::error::GqliteError::Storage(format!("fault injection: {}", msg)))
    } else {
        Ok(())
    }
}
