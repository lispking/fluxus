//! Fluxus API - High-level interface for stream processing
//!
//! This module provides a user-friendly API for building stream processing applications.

pub mod io;
pub mod operators;
pub mod source;
pub mod stream;

use fluxus_core::{Operator, Source};
pub use io::{CollectionSink, CollectionSource};
pub use stream::{DataStream, WindowedStream};

type InnerSource<T> = dyn Source<T> + Send + Sync;
type InnerOperator<T, R> = dyn Operator<T, R> + Send + Sync;
