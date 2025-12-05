use log::{debug, info};
use rusqlite::InterruptHandle;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

/// Represents a timeout entry in the queue
struct TimeoutEntry {
    interrupt_handle: Arc<InterruptHandle>,
}

/// Timeout queue that manages worker interruptions
pub struct TimeoutQueue {
    entries: Arc<Mutex<HashMap<String, TimeoutEntry>>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl TimeoutQueue {
    pub fn new() -> Self {
        let entries = Arc::new(Mutex::new(HashMap::new()));
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        Self {
            entries,
            shutdown_flag,
        }
    }

    /// Add a new timeout entry to the queue
    pub fn add_timeout(
        &self,
        interrupt_handle: Arc<InterruptHandle>,
        timeout_duration: Duration,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let entry = TimeoutEntry { interrupt_handle };

        self.entries.lock().unwrap().insert(id.clone(), entry);
        debug!(
            "Added timeout entry {} with duration {:?}",
            id, timeout_duration
        );

        // Start a dedicated thread for this timeout
        let entries = Arc::clone(&self.entries);
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let timeout_id = id.clone();

        thread::spawn(move || {
            // Sleep for the timeout duration
            thread::sleep(timeout_duration);

            // Check if shutdown was requested or entry was removed
            if shutdown_flag.load(Ordering::SeqCst) {
                return;
            }

            // Check if the entry still exists (might have been removed if query completed)
            let entry_exists = {
                let entries_lock = entries.lock().unwrap();
                entries_lock.contains_key(&timeout_id)
            };

            if entry_exists {
                debug!(
                    "Timeout expired for entry {}, interrupting query",
                    timeout_id
                );
                // The interrupt_handle is still valid, so we can interrupt
                if let Some(entry) = entries.lock().unwrap().remove(&timeout_id) {
                    entry.interrupt_handle.interrupt();
                }
            }
        });

        id
    }

    /// Remove a timeout entry from the queue
    pub fn remove_timeout(&self, id: &str) {
        self.entries.lock().unwrap().remove(id);
        debug!("Removed timeout entry {}", id);
    }

    /// Start the timeout monitoring thread (simplified - just for cleanup)
    pub fn start_monitoring(&self) {
        // No complex monitoring needed - each timeout has its own thread
        info!("Timeout queue initialized");
    }

    /// Stop the timeout monitoring thread
    pub fn stop_monitoring(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        info!("Timeout queue shutdown initiated");
    }
}
