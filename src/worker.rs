use crate::error::ApiError;
use crate::timeout::TimeoutQueue;
use base64::Engine;
use log::{debug, error, info};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub success: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub query_time_ms: Option<u64>,
}

pub struct Worker {
    id: usize,
    db_pool: Arc<crate::pool::DbPool>,
    max_query_time: Duration,
    timeout_queue: Arc<TimeoutQueue>,
    result_sender: mpsc::Sender<(String, QueryResult)>,
}

impl Worker {
    pub fn new(
        id: usize,
        db_pool: Arc<crate::pool::DbPool>,
        max_query_time: Duration,
        timeout_queue: Arc<TimeoutQueue>,
        result_sender: mpsc::Sender<(String, QueryResult)>,
    ) -> Self {
        info!("Worker {} initialized with connection pool", id);

        Self {
            id,
            db_pool,
            max_query_time,
            timeout_queue,
            result_sender,
        }
    }

    pub async fn execute_query(&self, query_id: String, query: String) -> Result<(), ApiError> {
        debug!("Worker {} executing query: {}", self.id, query);

        let start_time = Instant::now();
        let max_query_time = self.max_query_time;

        // Get connection from pool
        let connection = crate::pool::get_connection_from_pool(&self.db_pool)?;

        // Add timeout entry to the queue
        let interrupt_handle = Arc::new(connection.get_interrupt_handle());
        let timeout_id = self
            .timeout_queue
            .add_timeout(Arc::clone(&interrupt_handle), max_query_time);

        // Execute the query with timeout monitoring
        let result = tokio::task::spawn_blocking(move || {
            let mut stmt = match connection.prepare(&query) {
                Ok(stmt) => stmt,
                Err(e) => return Err(e),
            };
            let column_count = stmt.column_count();
            let mut columns = Vec::with_capacity(column_count);
            for i in 0..column_count {
                columns.push(stmt.column_name(i).unwrap_or("").to_string());
            }

            let mut rows = match stmt.query([]) {
                Ok(rows) => rows,
                Err(e) => return Err(e),
            };

            let mut data = Vec::new();

            while let Some(row) = match rows.next() {
                Ok(row) => row,
                Err(e) => return Err(e),
            } {
                let mut row_data = Vec::new();
                for i in 0..columns.len() {
                    let value: serde_json::Value = match row.get_ref(i) {
                        Ok(ref_val) => match ref_val.data_type() {
                            rusqlite::types::Type::Null => serde_json::Value::Null,
                            rusqlite::types::Type::Integer => serde_json::Value::Number(
                                row.get::<_, i64>(i)
                                    .map_or_else(|_e| serde_json::Number::from(0), |v| v.into()),
                            ),
                            rusqlite::types::Type::Real => serde_json::Value::Number(
                                serde_json::Number::from_f64(row.get::<_, f64>(i).unwrap_or(0.0))
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                            rusqlite::types::Type::Text => serde_json::Value::String(
                                row.get(i).map_or_else(|_| "".to_string(), |v| v),
                            ),
                            rusqlite::types::Type::Blob => {
                                let bytes: Vec<u8> = row.get(i).map_or_else(|_| Vec::new(), |v| v);
                                serde_json::Value::String(
                                    base64::engine::general_purpose::STANDARD.encode(bytes),
                                )
                            }
                        },
                        Err(e) => return Err(e),
                    };
                    row_data.push(value);
                }
                data.push(serde_json::Value::Array(row_data));
            }

            Ok(serde_json::json!({
                "columns": columns,
                "rows": data,
            }))
        })
        .await;

        // Remove the timeout entry from the queue
        self.timeout_queue.remove_timeout(&timeout_id);

        // Send result back to handler
        match result {
            Ok(Ok(query_result)) => {
                let result = QueryResult {
                    success: true,
                    result: Some(query_result),
                    error: None,
                    query_time_ms: Some(start_time.elapsed().as_millis() as u64),
                };
                self.result_sender
                    .send((query_id, result))
                    .await
                    .map_err(|e| ApiError::WorkerError(format!("Failed to send result: {}", e)))
            }
            Ok(Err(e)) => {
                let result = QueryResult {
                    success: false,
                    result: None,
                    error: Some(format!("Query execution failed: {}", e)),
                    query_time_ms: Some(start_time.elapsed().as_millis() as u64),
                };
                self.result_sender
                    .send((query_id, result))
                    .await
                    .map_err(|e| ApiError::WorkerError(format!("Failed to send result: {}", e)))
            }
            Err(e) => {
                // Check if the error is due to query interruption
                if format!("{}", e).contains("interrupted")
                    || format!("{}", e).contains("Interrupted")
                {
                    let result = QueryResult {
                        success: false,
                        result: None,
                        error: Some("Query timeout".to_string()),
                        query_time_ms: Some(max_query_time.as_millis() as u64),
                    };
                    self.result_sender
                        .send((query_id, result))
                        .await
                        .map_err(|e| ApiError::WorkerError(format!("Failed to send result: {}", e)))
                } else {
                    let result = QueryResult {
                        success: false,
                        result: None,
                        error: Some(format!("Query execution failed: {}", e)),
                        query_time_ms: Some(start_time.elapsed().as_millis() as u64),
                    };
                    self.result_sender
                        .send((query_id, result))
                        .await
                        .map_err(|e| ApiError::WorkerError(format!("Failed to send result: {}", e)))
                }
            }
        }
    }
}

pub struct WorkerPool {
    workers: Vec<Worker>,
    available_workers: VecDeque<usize>,
    query_receiver: mpsc::Receiver<(String, String)>,
    result_sender: mpsc::Sender<(String, QueryResult)>,
}

impl WorkerPool {
    pub fn new(
        worker_count: usize,
        max_query_time: Duration,
        timeout_queue: Arc<TimeoutQueue>,
        db_pool: Arc<crate::pool::DbPool>,
    ) -> (
        Self,
        mpsc::Sender<(String, String)>,
        mpsc::Receiver<(String, QueryResult)>,
    ) {
        let mut workers = Vec::with_capacity(worker_count);
        let mut available_workers = VecDeque::with_capacity(worker_count);

        // Create channels for query communication
        let (query_sender, query_receiver) = mpsc::channel(worker_count);
        let (result_sender_for_workers, result_receiver_for_handler) = mpsc::channel(worker_count);

        for i in 0..worker_count {
            let worker = Worker::new(
                i,
                Arc::clone(&db_pool),
                max_query_time,
                Arc::clone(&timeout_queue),
                result_sender_for_workers.clone(),
            );
            workers.push(worker);
            available_workers.push_back(i);
        }

        let worker_pool = Self {
            workers,
            available_workers,
            query_receiver,
            result_sender: result_sender_for_workers,
        };

        (worker_pool, query_sender, result_receiver_for_handler)
    }

    pub async fn run(&mut self) {
        info!("Worker pool started with {} workers", self.workers.len());

        // Add debug logging to see if we're even entering the loop
        debug!("Worker pool waiting for queries...");

        while let Some((query_id, query)) = self.query_receiver.recv().await {
            // Check if this is a shutdown message
            if query_id == "SHUTDOWN" {
                debug!("Received shutdown message, breaking out of worker loop");
                break;
            }

            debug!("Received query for execution: {}", query);

            // Get available worker from queue
            if let Some(worker_index) = self.available_workers.pop_front() {
                let worker = &self.workers[worker_index];
                let query_clone = query.clone();
                let query_id_clone = query_id.clone();
                let worker_id = worker.id;

                debug!("Worker {} assigned to execute query", worker_id);

                // Use the existing worker connection instead of creating a new one
                let result = worker.execute_query(query_id_clone, query_clone).await;

                // Return worker to available pool when done
                self.available_workers.push_back(worker_index);

                match result {
                    Ok(_) => {
                        debug!("Worker {} completed query successfully", worker_id);
                    }
                    Err(e) => {
                        error!("Worker {} failed to execute query: {}", worker_id, e);
                        // If worker fails, send an error result to prevent hanging
                        let error_result = QueryResult {
                            success: false,
                            result: None,
                            error: Some(format!("Worker {} failed: {}", worker_id, e)),
                            query_time_ms: None,
                        };
                        if let Err(send_err) = self
                            .result_sender
                            .send((query_id.clone(), error_result))
                            .await
                        {
                            error!(
                                "Failed to send error result for query {}: {}",
                                query_id, send_err
                            );
                        }
                    }
                };
            } else {
                error!("No available workers to handle query: {}", query);
                // In a real implementation, we might queue this or return an error
            }
        }

        info!("Worker pool shutting down");
    }
}
