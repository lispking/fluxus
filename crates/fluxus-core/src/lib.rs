//! Fluxus Core - A Flink-like stream processing engine in Rust
//!
//! This module contains the core abstractions and data types for stream processing.

pub mod config;
pub mod error_handling;
pub mod metrics;
pub mod models;
pub mod operator;
pub mod pipeline;
pub mod sink;
pub mod source;
pub mod window;

// Re-export commonly used items
pub use config::ParallelConfig;
pub use error_handling::{
    BackpressureController, BackpressureStrategy, ErrorHandler, RetryStrategy,
};
pub use metrics::{Counter, Gauge, MetricValue, Metrics, Timer};
pub use models::{Record, StreamError, StreamResult};
pub use operator::{Operator, OperatorBuilder};
pub use pipeline::Pipeline;
pub use sink::Sink;
pub use source::Source;
pub use window::WindowConfig;
// Test comment
