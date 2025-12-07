use crate::error::ApiError;
use base64::Engine;
use log::debug;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use warp::{Filter, Rejection, Reply};

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub success: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub query_time_ms: Option<u64>,
}

pub fn create_routes(
    shutdown_flag: Arc<AtomicBool>,
    max_query_time_ms: u64,
    db_pool: crate::pool::DbPool,
    timeout_queue: Arc<crate::timeout::TimeoutCollection>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    // POST /query endpoint
    let query_route = warp::path("query")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_shutdown_flag(shutdown_flag))
        .and(with_db_pool(db_pool))
        .and(with_timeout_queue(timeout_queue))
        .and(with_max_query_time(max_query_time_ms))
        .and_then(handle_query);

    // Health check endpoint
    let health_route = warp::path("health").and(warp::get()).map(move || {
        warp::reply::json(&serde_json::json!({
            "status": "healthy",
            "max_query_time_ms": max_query_time_ms
        }))
    });

    // Combine routes
    query_route.or(health_route)
}

fn with_shutdown_flag(
    shutdown_flag: Arc<AtomicBool>,
) -> impl Filter<Extract = (Arc<AtomicBool>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || shutdown_flag.clone())
}

fn with_db_pool(
    db_pool: crate::pool::DbPool,
) -> impl Filter<Extract = (crate::pool::DbPool,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || db_pool.clone())
}

fn with_timeout_queue(
    timeout_queue: Arc<crate::timeout::TimeoutCollection>,
) -> impl Filter<Extract = (Arc<crate::timeout::TimeoutCollection>,), Error = std::convert::Infallible>
       + Clone {
    warp::any().map(move || timeout_queue.clone())
}

fn with_max_query_time(
    max_query_time_ms: u64,
) -> impl Filter<Extract = (u64,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || max_query_time_ms)
}

async fn handle_query(
    query_request: QueryRequest,
    shutdown_flag: Arc<AtomicBool>,
    db_pool: crate::pool::DbPool,
    timeout_queue: Arc<crate::timeout::TimeoutCollection>,
    max_query_time_ms: u64,
) -> Result<impl Reply, Rejection> {
    debug!("Received query request: {}", query_request.sql);

    // Check if shutdown is in progress
    if shutdown_flag.load(Ordering::SeqCst) {
        debug!("Shutdown in progress, returning 503 Service Unavailable");
        let response = QueryResponse {
            success: false,
            result: None,
            error: Some("Service is shutting down".to_string()),
            query_time_ms: None,
        };
        return Ok(warp::reply::with_status(
            warp::reply::json(&response),
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        ));
    }

    // Execute query directly with timeout management
    let _query_id = Uuid::new_v4().to_string();
    let max_query_time = Duration::from_millis(max_query_time_ms);

    // Get connection from pool
    let connection = match crate::pool::get_connection_from_pool(&db_pool) {
        Ok(conn) => conn,
        Err(e) => {
            let response = QueryResponse {
                success: false,
                result: None,
                error: Some(format!("Failed to get database connection: {}", e)),
                query_time_ms: None,
            };
            return Ok(warp::reply::with_status(
                warp::reply::json(&response),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    };

    // Add timeout entry to the queue
    let interrupt_handle = Arc::new(connection.get_interrupt_handle());
    let timeout_id = timeout_queue.add_timeout(Arc::clone(&interrupt_handle), max_query_time);

    // Execute the query with timeout monitoring
    let start_time = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let mut stmt = match connection.prepare(&query_request.sql) {
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
    timeout_queue.remove_timeout(&timeout_id);

    // Process the result
    let result = match result {
        Ok(Ok(query_result)) => {
            let query_time_ms = Some(start_time.elapsed().as_millis() as u64);
            Ok((query_result, query_time_ms))
        }
        Ok(Err(e)) => {
            // Check if the error is due to query interruption
            if format!("{}", e).contains("interrupted") || format!("{}", e).contains("Interrupted")
            {
                Err(ApiError::WorkerError("Query timeout".to_string()))
            } else {
                Err(ApiError::WorkerError(format!(
                    "Query execution failed: {}",
                    e
                )))
            }
        }
        Err(e) => {
            // Check if the error is due to query interruption
            if format!("{}", e).contains("interrupted") || format!("{}", e).contains("Interrupted")
            {
                Err(ApiError::WorkerError("Query timeout".to_string()))
            } else {
                Err(ApiError::WorkerError(format!(
                    "Query execution failed: {}",
                    e
                )))
            }
        }
    };

    match result {
        Ok((query_result, query_time_ms)) => {
            let response = QueryResponse {
                success: true,
                result: Some(query_result),
                error: None,
                query_time_ms: query_time_ms,
            };
            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                warp::http::StatusCode::OK,
            ))
        }
        Err(e) => {
            let response = QueryResponse {
                success: false,
                result: None,
                error: Some(format!("Query execution failed: {}", e)),
                query_time_ms: None,
            };
            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}
