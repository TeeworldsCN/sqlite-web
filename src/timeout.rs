use log::{debug, info};
use rusqlite::InterruptHandle;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Represents a timeout entry in the queue
struct TimeoutEntry {
    interrupt_handle: Arc<InterruptHandle>,
}

/// Timeout queue that manages worker interruptions
pub struct TimeoutCollection {
    entries: Arc<Mutex<HashMap<usize, TimeoutEntry>>>,
    shutdown_flag: Arc<AtomicBool>,
    next_id: AtomicUsize,
}

impl TimeoutCollection {
    pub fn new() -> Self {
        let entries = Arc::new(Mutex::new(HashMap::new()));
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let next_id = AtomicUsize::new(1);

        Self {
            entries,
            shutdown_flag,
            next_id,
        }
    }

    /// Add a new timeout entry to the queue
    pub fn add_timeout(
        &self,
        interrupt_handle: Arc<InterruptHandle>,
        timeout_duration: Duration,
    ) -> usize {
        // Make the entire add operation atomic by locking first
        let mut entries_lock = self.entries.lock().unwrap();

        // If shutdown was requested, don't add new timeouts
        if self.shutdown_flag.load(Ordering::SeqCst) {
            return 0;
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let entry = TimeoutEntry { interrupt_handle };

        entries_lock.insert(id, entry);
        debug!(
            "Added timeout entry {} with duration {:?}",
            id, timeout_duration
        );

        // Release the lock before spawning the thread to avoid holding it during thread creation
        drop(entries_lock);

        // Start a dedicated thread for this timeout
        let entries = Arc::clone(&self.entries);
        let timeout_id = id;

        thread::spawn(move || {
            // Sleep for the timeout duration
            thread::sleep(timeout_duration);

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
    pub fn remove_timeout(&self, id: usize) {
        let mut entries_lock = self.entries.lock().unwrap();
        entries_lock.remove(&id);
        debug!("Removed timeout entry {}", id);
        // Lock is automatically released when entries_lock goes out of scope
    }

    /// Stop the timeout monitoring thread
    pub fn interrupt_all(&self) {
        let mut entries_lock = self.entries.lock().unwrap();
        info!("Timeout interrupt shutdown initiated");

        self.shutdown_flag.store(true, Ordering::SeqCst);

        for (_, entry) in entries_lock.drain() {
            entry.interrupt_handle.interrupt();
        }
    }
}
