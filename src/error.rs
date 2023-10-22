use std::io;
use std::string::FromUtf8Error;
use thiserror::Error;

/// Error type for GrausDb.
#[derive(Error, Debug)]
pub enum GrausError {
    /// IO Error
    #[error("GrausDb IO error")]
    Io(#[from] io::Error),
    /// Removing non-existent key error.
    #[error("Key not found")]
    KeyNotFound,
    /// Serialization or deserialization error.
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[error("Unexpected command type")]
    UnexpectedCommandType,
    /// Key or value is invalid UTF-8 sequence
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] FromUtf8Error),
    /// Error with a string message
    #[error("{0}")]
    StringError(String),
    /// Predicate passed to update_if was not satisfied.
    #[error("Predicate not satisfied")]
    PredicateNotSatisfied,
}

/// Result type for GrausDb.
pub type Result<T> = std::result::Result<T, GrausError>;
