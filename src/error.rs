use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Server error: {0}")]
    ThreadError(#[from] JoinError),

    #[error("Query error: {0}")]
    QueryError(#[from] rusqlite::Error),

    #[error("Queue error: {0}")]
    QueueError(#[from] r2d2::Error),
}
