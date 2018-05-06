use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

/// A resource is considered idle when it is not being used to process messages.
pub trait IsIdle {
    fn is_idle(&self) -> bool;
}

/// Counts the number of active messages to determine idleness.
#[derive(Debug, Default, Clone)]
pub struct Idle(Arc<AtomicUsize>);

/// A handle that decrements the number of active messages on drop.
#[derive(Debug)]
pub struct Active(Option<Arc<AtomicUsize>>);

// ===== impl Idle =====

impl Idle {
    pub fn active(&mut self) -> Active {
        self.0.fetch_add(1, Ordering::AcqRel);
        Active(Some(self.0.clone()))
    }
}

impl IsIdle for Idle {
    fn is_idle(&self) -> bool {
        self.0.load(Ordering::Acquire) == 0
    }
}

// ===== impl Active =====

impl Drop for Active {
    fn drop(&mut self) {
        if let Some(active) = self.0.take() {
            active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[test]
fn not_idle_while_active() {
    let mut idle = Idle::default();
    let act0 = idle.active();
    assert!(!idle.is_idle());
    let act1 = idle.active();
    assert!(!idle.is_idle());
    drop(act0);
    assert!(!idle.is_idle());
    drop(act1);
    assert!(idle.is_idle());
}
