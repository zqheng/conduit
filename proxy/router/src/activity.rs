
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

pub trait HasActivity {
    fn activity(&self) -> &Activity;
}

#[derive(Default, Debug)]
pub struct Activity {
    pending_requests: AtomicUsize,
    active_responses: AtomicUsize,
}

#[derive(Debug, Default, Clone)]
pub struct TrackActivity(Arc<Activity>);

#[derive(Debug)]
pub struct PendingRequest(Option<Arc<Activity>>);

#[derive(Debug)]
pub struct ActiveResponse(Option<Arc<Activity>>);

// ===== impl Activity =====

impl Activity {
    pub fn pending_requests(&self) -> usize {
        self.pending_requests.load(Ordering::Release)
    }

    pub fn active_responses(&self) -> usize {
        self.active_responses.load(Ordering::Release)
    }
}

// ===== impl TrackActivity =====

impl TrackActivity {
    pub fn pending_request(&self) -> PendingRequest {
        self.0.pending_requests.fetch_add(1, Ordering::AcqRel);
        PendingRequest(Some(self.0.clone()))
    }

    pub fn active_response(&self) -> ActiveResponse {
        self.0.active_responses.fetch_add(1, Ordering::AcqRel);
        ActiveResponse(Some(self.0.clone()))
    }
}

impl HasActivity for TrackActivity {
    fn activity(&self) -> &Activity {
        &self.0
    }
}

// ===== impl PendingRequest =====

impl Drop for PendingRequest {
    fn drop(&mut self) {
        if let Some(active) = self.0.take() {
            active.pending_requests.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

// ===== impl ActiveResponse =====

impl Drop for ActiveResponse {
    fn drop(&mut self) {
        if let Some(active) = self.0.take() {
            active.active_responses.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
