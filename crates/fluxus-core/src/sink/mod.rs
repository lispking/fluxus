pub mod buffered;
pub mod console;
pub mod error_converters;
pub mod file;

pub use buffered::BufferedSink;
pub use console::ConsoleSink;
pub use file::FileSink;

use crate::models::{Record, StreamResult};
use async_trait::async_trait;
use std::fmt::Display;

/// Sink trait defines the interface for data output
#[async_trait]
pub trait Sink<T> {
    /// Initialize the sink
    async fn init(&mut self) -> StreamResult<()>;

    /// Write a record to the sink
    async fn write(&mut self, record: Record<T>) -> StreamResult<()>;

    /// Flush any buffered data
    async fn flush(&mut self) -> StreamResult<()>;

    /// Close the sink and release resources
    async fn close(&mut self) -> StreamResult<()>;
}

/// Formatter for console output
pub trait ConsoleFormatter<T> {
    fn format(&self, record: &Record<T>) -> String;
}

/// Default formatter that uses Display
pub struct DefaultFormatter;

impl<T: Display> ConsoleFormatter<T> for DefaultFormatter {
    fn format(&self, record: &Record<T>) -> String {
        format!("[{}] {}", record.timestamp, record.data)
    }
}
