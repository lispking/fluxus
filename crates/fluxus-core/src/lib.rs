//! Fluxus Core - A Flink-like stream processing engine in Rust
//!
//! This module contains the core abstractions and data types for stream processing.

pub mod config;
pub mod error_handling;
pub mod metrics;
pub mod pipeline;

// Re-export commonly used items
pub use config::ParallelConfig;
pub use error_handling::{
    BackpressureController, BackpressureStrategy, ErrorHandler, RetryStrategy,
};
pub use metrics::{Counter, Gauge, MetricValue, Metrics, Timer};
pub use pipeline::Pipeline;
