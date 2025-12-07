use anyhow::Result;
use log::info;
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
    let _ = handler::start_server(
        config.max_query_time_ms,
        db_pool,
        config.listen_address.clone(),
        config.listen_port,
    )
    .await;

    info!("Server shutdown complete");

    Ok(())
}
