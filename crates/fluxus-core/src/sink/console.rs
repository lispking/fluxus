use crate::models::{Record, StreamResult};
use crate::sink::{ConsoleFormatter, DefaultFormatter, Sink};
use async_trait::async_trait;
use std::marker::PhantomData;

/// A sink that writes to console
#[derive(Default)]
pub struct ConsoleSink<T, F = DefaultFormatter> {
    formatter: F,
    _phantom: PhantomData<T>,
}

impl<T> ConsoleSink<T, DefaultFormatter> {
    /// Create a new console sink with default formatter
    pub fn new() -> Self {
        Self {
            formatter: DefaultFormatter,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> ConsoleSink<T, F> {
    /// Create a new console sink with custom formatter
    pub fn with_formatter(formatter: F) -> Self {
        Self {
            formatter,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, F> Sink<T> for ConsoleSink<T, F>
where
    T: Send,
    F: ConsoleFormatter<T> + Send + Sync,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn write(&mut self, record: Record<T>) -> StreamResult<()> {
        tracing::info!("{}", self.formatter.format(&record));
        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
