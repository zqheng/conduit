
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

pub trait IsIdle {
    fn is_idle(&self) -> bool;
}

#[derive(Debug, Default, Clone)]
pub struct TrackActivity(Arc<AtomicUsize>);

#[derive(Debug)]
pub struct Active(Option<Arc<AtomicUsize>>);

// ===== impl TrackActivity =====

impl TrackActivity {
    pub fn active(&self) -> Active {
        self.0.fetch_add(1, Ordering::AcqRel);
        Active(Some(self.0.clone()))
    }
}

impl IsIdle for TrackActivity {
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
