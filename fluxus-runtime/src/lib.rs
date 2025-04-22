//! Fluxus Runtime - Execution engine for stream processing
//!
//! This module implements the runtime execution environment for Fluxus pipelines.
mod runtime;
pub use runtime::*;

/// State management for stateful operators
pub mod state;

/// Watermark tracking and propagation
pub mod watermark;
