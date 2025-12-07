use anyhow::Result;
use log::info;
use std::net::SocketAddr;
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
    let timeout_queue = Arc::new(timeout::TimeoutCollection::new());
    timeout_queue.start_monitoring();

    // Create shutdown flag
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Create connection pool
    let db_pool =
        pool::create_connection_pool(&config.database_path, config.max_concurrent_queries as u32)?;

    // Set up HTTP routes with connection pool and shutdown flag
    let routes = handler::create_routes(
        Arc::clone(&shutdown_flag),
        config.max_query_time_ms,
        db_pool,
        Arc::clone(&timeout_queue),
    );

    // Start HTTP server
    info!(
        "Starting HTTP server on {}:{}",
        config.listen_address, config.listen_port
    );
    let socket_addr: SocketAddr = format!("{}:{}", config.listen_address, config.listen_port)
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid socket address: {}", e))?;

    warp::serve(routes)
        .bind(socket_addr)
        .await
        .graceful(async move {
            let _ = tokio::signal::ctrl_c().await;

            info!("Received shutdown signal, initiating graceful shutdown...");

            // Stop timeout queue monitoring
            timeout_queue.stop_monitoring();

            // Set shutdown flag to prevent new queries
            shutdown_flag.store(true, Ordering::SeqCst);

            // Give a brief moment for in-flight responses to complete (100ms)
            tokio::time::sleep(Duration::from_millis(100)).await;

            info!("Graceful shutdown complete");
        })
        .run()
        .await;

    info!("Server shutdown complete");

    Ok(())
}
