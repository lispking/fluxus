use thiserror::Error;

use crate::time::current_time;

/// Record represents a single data record in the stream
#[derive(Debug, Clone)]
pub struct Record<T> {
    /// The actual data payload
    pub data: T,
    /// Timestamp of the record (in milliseconds)
    pub timestamp: i64,
}

impl<T> Record<T> {
    /// Create a new record with the current timestamp
    pub fn new(data: T) -> Self {
        let timestamp = current_time() as i64;
        Record { data, timestamp }
    }

    /// Create a new record with a specific timestamp
    pub fn with_timestamp(data: T, timestamp: i64) -> Self {
        Record { data, timestamp }
    }
}

/// Error types that can occur during stream processing
#[derive(Error, Debug)]
pub enum StreamError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("EOF")]
    EOF,

    #[error("Wait for {0} milliseconds")]
    Wait(u64),
}

/// A Result type specialized for stream processing operations
pub type StreamResult<T> = Result<T, StreamError>;
