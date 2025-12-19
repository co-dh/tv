//! Custom error types for tv

use thiserror::Error;

/// Domain-specific errors for tv
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TvError {
    #[error("no table loaded")]
    NoTable,

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
