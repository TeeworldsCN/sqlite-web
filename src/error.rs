use thiserror::Error;
use warp::reject::Reject;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("SQLite error: {0}")]
    SqliteError(#[from] rusqlite::Error),

    #[error("Worker pool error: {0}")]
    WorkerError(String),
}

impl Reject for ApiError {}
