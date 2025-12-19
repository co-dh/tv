//! Custom error types for tv

use thiserror::Error;

/// Domain-specific errors for tv
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TvError {
    #[error("no table loaded")]
    NoTable,

    #[error("column '{0}' not found")]
    ColumnNotFound(String),

    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    #[error("loading in progress")]
    Loading,

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

impl From<&str> for TvError {
    fn from(s: &str) -> Self { Self::Other(s.into()) }
}

impl From<String> for TvError {
    fn from(s: String) -> Self { Self::Other(s) }
}

/// Result type alias using TvError
pub type TvResult<T> = Result<T, TvError>;
