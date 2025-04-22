//! Fluxus Core - A Flink-like stream processing engine in Rust
//! 
//! This module contains the core abstractions and data types for stream processing.

use std::time::SystemTime;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use thiserror::Error;

/// Represents a record in a data stream with timestamp and value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record<T: Clone> {
    /// Timestamp of the record
    pub timestamp: SystemTime,
    /// Actual data payload
    pub value: T,
    /// Optional watermark for event time processing
    pub watermark: Option<SystemTime>,
}

/// Error types for stream processing operations
#[derive(Error, Debug)]
pub enum StreamError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Processing error: {0}")]
    Processing(String),
}

/// Result type for stream processing operations
pub type StreamResult<T> = Result<T, StreamError>;

/// Trait for data sources that produce records
#[async_trait]
pub trait Source<T: Clone> {
    /// Retrieves the next record from the source
    async fn next(&mut self) -> StreamResult<Option<Record<T>>>;
    /// Closes the source
    async fn close(&mut self) -> StreamResult<()>;
}

/// Trait for data sinks that consume records
#[async_trait]
pub trait Sink<T: Clone> {
    /// Writes a record to the sink
    async fn write(&mut self, record: Record<T>) -> StreamResult<()>;
    /// Flushes any buffered records
    async fn flush(&mut self) -> StreamResult<()>;
    /// Closes the sink
    async fn close(&mut self) -> StreamResult<()>;
}

/// Trait for stream operators that transform data
#[async_trait]
pub trait Operator<In: Clone, Out: Clone>: Send + Sync {
    /// Processes a single record and produces output records
    async fn process(&self, record: Record<In>) -> StreamResult<Vec<Record<Out>>>;
    /// Called when a watermark arrives
    async fn on_watermark(&self, watermark: SystemTime) -> StreamResult<()>;
}

/// Configuration for parallel execution
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of parallel tasks
    pub parallelism: usize,
    /// Buffer size between operators
    pub buffer_size: usize,
}

/// Window configuration for stream processing
#[derive(Debug, Clone)]
pub enum WindowConfig {
    /// Tumbling window with fixed size
    Tumbling { size_ms: u64 },
    /// Sliding window with size and slide interval
    Sliding { size_ms: u64, slide_ms: u64 },
    /// Session window with timeout
    Session { timeout_ms: u64 },
}

/// Pipeline builder for constructing stream processing workflows
pub mod pipeline {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone)]
    pub struct Pipeline<T> {
        pub(crate) nodes: Vec<Arc<dyn Operator<T, T> + Send + Sync>>,
        pub(crate) parallel_config: Option<ParallelConfig>,
    }

    impl<T> Pipeline<T> 
    where
        T: Clone + Send + Sync + 'static,
    {
        pub fn new() -> Self {
            Self {
                nodes: Vec::new(),
                parallel_config: None,
            }
        }

        pub fn add_operator<O>(mut self, operator: O) -> Self 
        where
            O: Operator<T, T> + Send + Sync + 'static,
        {
            self.nodes.push(Arc::new(operator));
            self
        }

        pub fn set_parallelism(mut self, config: ParallelConfig) -> Self {
            self.parallel_config = Some(config);
            self
        }
    }
}
