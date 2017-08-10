use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct CompletionHandler {
    completed: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
    started: Arc<AtomicBool>,
}

impl CompletionHandler {
    pub fn new() -> CompletionHandler {
        CompletionHandler {
            completed: Arc::new(AtomicUsize::new(0)),
            total: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn start(&self, countdown: usize) {
        self.total.store(countdown, Ordering::Relaxed);
        self.started.store(true, Ordering::Relaxed);
    }

    pub fn is_complete(&self) -> bool {
        if self.started.load(Ordering::Relaxed) == false {
            return false
        }

        if self.completed.load(Ordering::Relaxed)
            == self.total.load(Ordering::Relaxed) {
            true
        } else {
            false
        }
    }

    pub fn success(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn failed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}
