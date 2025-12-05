use crate::error::ApiError;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    query_sender: tokio::sync::mpsc::Sender<(String, String)>,
    result_receiver: std::sync::Arc<
        tokio::sync::Mutex<tokio::sync::mpsc::Receiver<(String, crate::worker::QueryResult)>>,
    >,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    // POST /query endpoint
    let query_route = warp::path("query")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_shutdown_flag(shutdown_flag))
        .and(with_query_sender(query_sender))
        .and(with_result_receiver(result_receiver))
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

fn with_query_sender(
    query_sender: tokio::sync::mpsc::Sender<(String, String)>,
) -> impl Filter<
    Extract = (tokio::sync::mpsc::Sender<(String, String)>,),
    Error = std::convert::Infallible,
> + Clone {
    warp::any().map(move || query_sender.clone())
}

fn with_result_receiver(
    result_receiver: std::sync::Arc<
        tokio::sync::Mutex<tokio::sync::mpsc::Receiver<(String, crate::worker::QueryResult)>>,
    >,
) -> impl Filter<
    Extract = (
        std::sync::Arc<
            tokio::sync::Mutex<tokio::sync::mpsc::Receiver<(String, crate::worker::QueryResult)>>,
        >,
    ),
    Error = std::convert::Infallible,
> + Clone {
    warp::any().map(move || result_receiver.clone())
}

async fn handle_query(
    query_request: QueryRequest,
    shutdown_flag: Arc<AtomicBool>,
    query_sender: tokio::sync::mpsc::Sender<(String, String)>,
    result_receiver: std::sync::Arc<
        tokio::sync::Mutex<tokio::sync::mpsc::Receiver<(String, crate::worker::QueryResult)>>,
    >,
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

    // Send query to worker pool with query ID and wait for result
    let query_clone = query_request.sql.clone();
    let query_id = Uuid::new_v4().to_string();
    if let Err(e) = query_sender.send((query_id.clone(), query_clone)).await {
        error!("Failed to send query to worker pool: {}", e);
        let response = QueryResponse {
            success: false,
            result: None,
            error: Some("Failed to send query to worker pool".to_string()),
            query_time_ms: None,
        };
        return Ok(warp::reply::with_status(
            warp::reply::json(&response),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }

    // Wait for the worker pool to return results
    let result = match result_receiver.lock().await.recv().await {
        Some((received_query_id, query_result)) => {
            if received_query_id == query_id {
                if query_result.success {
                    Ok((
                        query_result.result.unwrap_or_else(|| serde_json::json!({})),
                        query_result.query_time_ms,
                    ))
                } else {
                    Err(ApiError::WorkerError(
                        query_result
                            .error
                            .unwrap_or_else(|| "Unknown error".to_string()),
                    ))
                }
            } else {
                // Wrong query ID, this shouldn't happen but handle it
                Err(ApiError::WorkerError(
                    "Received result for wrong query ID".to_string(),
                ))
            }
        }
        None => Err(ApiError::WorkerError(
            "No result received from worker pool".to_string(),
        )),
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
