use anyhow::Result;
use log::info;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
mod config;
mod error;
mod handler;
mod pool;
mod timeout;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();
    info!("Starting SQLite Web API Server");

    // Load configuration
    let config = config::Config::load()?;
    info!("Configuration loaded: {:?}", config);

    // Create timeout queue
    let timeout = Arc::new(timeout::TimeoutCollection::new());

    // Create shutdown flag
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Create connection pool
    let db_pool = pool::create_connection_pool(
        &config.database_path,
        config.max_concurrent_queries,
        config.max_queue_timeout_ms,
    )?;

    // Start HTTP server
    info!(
        "Starting HTTP server on {}:{}",
        config.listen_address, config.listen_port
    );

    // Start the server in a separate task
    let server_task = tokio::spawn(handler::start_server(
        Arc::clone(&shutdown_flag),
        config.max_query_time_ms,
        db_pool,
        Arc::clone(&timeout),
        config.listen_address.clone(),
        config.listen_port,
    ));

    // Wait for shutdown signal
    let _ = tokio::signal::ctrl_c().await;

    info!("Received shutdown signal, initiating graceful shutdown...");

    timeout.interrupt_all();

    // Set shutdown flag to prevent new queries
    shutdown_flag.store(true, Ordering::SeqCst);

    // Give a brief moment for in-flight responses to complete (100ms)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel the server task
    server_task.abort();

    info!("Graceful shutdown complete");

    info!("Server shutdown complete");

    Ok(())
}
