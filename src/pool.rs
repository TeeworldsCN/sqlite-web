use crate::error::ApiError;
use log::info;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::OpenFlags;
use std::time::Duration;

pub type DbPool = Pool<SqliteConnectionManager>;

pub fn create_connection_pool(database_path: &str, pool_size: u32) -> Result<DbPool, ApiError> {
    info!(
        "Creating connection pool for database: {} with {} connections",
        database_path, pool_size
    );

    // Create connection manager with read-only flags
    let manager =
        SqliteConnectionManager::file(database_path).with_flags(OpenFlags::SQLITE_OPEN_READ_ONLY);

    // Build the connection pool
    let pool = Pool::builder()
        .max_size(pool_size)
        .connection_timeout(Duration::from_secs(5))
        .build(manager)
        .map_err(|e| ApiError::WorkerError(format!("Failed to create connection pool: {}", e)))?;

    info!("Connection pool created successfully");
    Ok(pool)
}

pub fn get_connection_from_pool(
    pool: &DbPool,
) -> Result<r2d2::PooledConnection<SqliteConnectionManager>, ApiError> {
    pool.get()
        .map_err(|e| ApiError::WorkerError(format!("Failed to get connection from pool: {}", e)))
}
