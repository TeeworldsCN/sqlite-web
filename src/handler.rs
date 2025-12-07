use crate::error::ApiError;
use base64::Engine;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

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

pub struct AppState {
    pub shutdown_flag: Arc<AtomicBool>,
    pub max_query_time_ms: u64,
    pub db_pool: crate::pool::DbPool,
    pub timeout: Arc<crate::timeout::TimeoutCollection>,
}

pub async fn start_server(
    shutdown_flag: Arc<AtomicBool>,
    max_query_time_ms: u64,
    db_pool: crate::pool::DbPool,
    timeout: Arc<crate::timeout::TimeoutCollection>,
    listen_address: String,
    listen_port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", listen_address, listen_port)
        .parse()
        .map_err(|e: std::net::AddrParseError| format!("Invalid socket address: {}", e))?;

    let listener = TcpListener::bind(addr).await?;
    info!("Starting HTTP server on {}", addr);

    let state = Arc::new(AppState {
        shutdown_flag,
        max_query_time_ms,
        db_pool,
        timeout,
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let state = state.clone();

        tokio::spawn(async move {
            let disconnect_flag = Arc::new(AtomicBool::new(false));
            let disconnect_flag_clone = disconnect_flag.clone();
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        let state = state.clone();
                        let disconnect_flag = disconnect_flag.clone();
                        async move { handle_request(req, state, disconnect_flag).await }
                    }),
                )
                .with_upgrades()
                .await
            {
                disconnect_flag_clone.store(true, Ordering::SeqCst);
                debug!("Connection error: {}", err);

                // Check if this is a connection closure error
                if err.to_string().contains("connection closed")
                    || err.to_string().contains("reset")
                    || err.to_string().contains("broken pipe")
                {
                    debug!("Client disconnected abruptly");
                }
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<AppState>,
    disconnect_flag: Arc<AtomicBool>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path();
    let method = req.method();

    match (method, path) {
        (&Method::POST, "/query") => handle_query(req, state, disconnect_flag).await,
        (&Method::GET, "/health") => handle_health(state).await,
        _ => Ok(not_found_response()),
    }
}

async fn handle_health(state: Arc<AppState>) -> Result<Response<Full<Bytes>>, Infallible> {
    let response_body = serde_json::json!({
        "status": "healthy",
        "max_query_time_ms": state.max_query_time_ms
    });

    Ok(json_response(response_body, StatusCode::OK))
}

async fn handle_query(
    req: Request<Incoming>,
    state: Arc<AppState>,
    disconnect_flag: Arc<AtomicBool>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    // Read the request body
    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            debug!("Failed to read request body: {}", e);
            let response = QueryResponse {
                success: false,
                result: None,
                error: Some("Failed to read request body".to_string()),
                query_time_ms: None,
            };
            return Ok(json_response(response, StatusCode::BAD_REQUEST));
        }
    };

    // Parse JSON request
    let query_request: QueryRequest = match serde_json::from_slice(&body_bytes) {
        Ok(req) => req,
        Err(e) => {
            debug!("Failed to parse JSON request: {}", e);
            let response = QueryResponse {
                success: false,
                result: None,
                error: Some("Invalid JSON format".to_string()),
                query_time_ms: None,
            };
            return Ok(json_response(response, StatusCode::BAD_REQUEST));
        }
    };

    debug!("Received query request: {}", query_request.sql);

    // Check if shutdown is in progress
    if state.shutdown_flag.load(Ordering::SeqCst) {
        debug!("Shutdown in progress, returning 503 Service Unavailable");
        let response = QueryResponse {
            success: false,
            result: None,
            error: Some("Service is shutting down".to_string()),
            query_time_ms: None,
        };
        return Ok(json_response(response, StatusCode::SERVICE_UNAVAILABLE));
    }

    // Get connection from pool
    let connection = match crate::pool::get_connection_from_pool(&state.db_pool) {
        Ok(conn) => conn,
        Err(e) => {
            let response = QueryResponse {
                success: false,
                result: None,
                error: Some(e.to_string()),
                query_time_ms: None,
            };
            return Ok(json_response(response, StatusCode::INTERNAL_SERVER_ERROR));
        }
    };

    let max_query_time = Duration::from_millis(state.max_query_time_ms);

    // already disconnected, quickly return
    if disconnect_flag.load(Ordering::SeqCst) {
        debug!("Connection aborted, skip executing query");
        let response = QueryResponse {
            success: false,
            result: None,
            error: Some("Aborted".to_string()),
            query_time_ms: None,
        };
        return Ok(json_response(response, StatusCode::SERVICE_UNAVAILABLE));
    }

    // start timeout
    let interrupt_handle = Arc::new(connection.get_interrupt_handle());
    let timeout_id = state
        .timeout
        .add_timeout(Arc::clone(&interrupt_handle), max_query_time);

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
    state.timeout.remove_timeout(timeout_id);

    // Process the result
    let result = match result {
        Ok(Ok(query_result)) => {
            let query_time_ms = Some(start_time.elapsed().as_millis() as u64);
            Ok((query_result, query_time_ms))
        }
        Ok(Err(e)) => Err(ApiError::QueryError(e)),
        Err(e) => Err(ApiError::ThreadError(e)),
    };

    match result {
        Ok((query_result, query_time_ms)) => {
            let response = QueryResponse {
                success: true,
                result: Some(query_result),
                error: None,
                query_time_ms: query_time_ms,
            };
            Ok(json_response(response, StatusCode::OK))
        }
        Err(e) => {
            let response = QueryResponse {
                success: false,
                result: None,
                error: Some(e.to_string()),
                query_time_ms: None,
            };
            Ok(json_response(response, StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}

fn json_response(data: impl Serialize, status: StatusCode) -> Response<Full<Bytes>> {
    let json_body = match serde_json::to_vec(&data) {
        Ok(body) => body,
        Err(_) => {
            let error_response = serde_json::json!({
                "error": "Failed to serialize response"
            });
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::from(
                    serde_json::to_string(&error_response).unwrap_or_else(|_| {
                        "{\"error\":\"Failed to serialize response\"}".to_string()
                    }),
                ))
                .unwrap();
        }
    };

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::from(json_body))
        .unwrap()
}

fn not_found_response() -> Response<Full<Bytes>> {
    let response = serde_json::json!({
        "error": "Not found",
    });

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::from(
            serde_json::to_string(&response)
                .unwrap_or_else(|_| "{\"error\":\"Not found\"}".to_string()),
        ))
        .unwrap()
}
