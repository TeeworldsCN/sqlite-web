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
mod worker;

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
    let db_pool = Arc::new(pool::create_connection_pool(
        &config.database_path,
        config.worker_count as u32,
    )?);

    // Create worker pool
    let (mut worker_pool, query_tx, result_rx) = worker::WorkerPool::new(
        config.worker_count,
        Duration::from_millis(config.max_query_time_ms),
        Arc::clone(&timeout_queue),
        Arc::clone(&db_pool),
    );

    // Start worker pool in current thread
    let worker_task = tokio::spawn(async move {
        worker_pool.run().await;
    });

    // Set up HTTP routes with connection pool and shutdown flag
    let result_rx_arc = Arc::new(tokio::sync::Mutex::new(result_rx));
    let routes = handler::create_routes(
        Arc::clone(&shutdown_flag),
        config.max_query_time_ms,
        query_tx.clone(),
        Arc::clone(&result_rx_arc),
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
            // Send shutdown notification to worker pool
            let _ = query_tx
                .send(("SHUTDOWN".to_string(), "".to_string()))
                .await;

            // Give a brief moment for in-flight responses to complete (100ms)
            tokio::time::sleep(Duration::from_millis(100)).await;

            let _ = worker_task.await;

            // Close the connection pool
            drop(db_pool);

            info!("Graceful shutdown complete");
        })
        .run()
        .await;

    info!("Server shutdown complete");

    Ok(())
}
