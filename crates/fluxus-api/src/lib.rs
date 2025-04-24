//! Fluxus API - High-level interface for stream processing
//!
//! This module provides a user-friendly API for building stream processing applications.

pub mod io;
pub mod operators;
pub mod stream;

pub use io::{CollectionSink, CollectionSource};
pub use stream::{DataStream, WindowedStream};
