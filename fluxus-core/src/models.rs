use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Record represents a single data record in the stream
#[derive(Debug)]
pub struct Record<T> {
    /// The actual data payload
    pub data: T,
    /// Timestamp of the record (in milliseconds)
    pub timestamp: i64,
}

impl<T: Clone> Clone for Record<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            timestamp: self.timestamp,
        }
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
}

/// A Result type specialized for stream processing operations
pub type StreamResult<T> = Result<T, StreamError>;

impl<T> Record<T> {
    /// Create a new record with the current timestamp
    pub fn new(data: T) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        Record { data, timestamp }
    }

    /// Create a new record with a specific timestamp
    pub fn with_timestamp(data: T, timestamp: i64) -> Self {
        Record { data, timestamp }
    }
}
