use crate::Sink;
use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamResult};
use std::time::{Duration, Instant};

/// A sink wrapper that provides buffering capabilities
pub struct BufferedSink<T, S: Sink<T>> {
    inner: S,
    buffer: Vec<Record<T>>,
    buffer_size: usize,
    flush_interval: Duration,
    last_flush: Instant,
}

impl<T, S: Sink<T>> BufferedSink<T, S> {
    /// Create a new buffered sink with the specified buffer size and flush interval
    pub fn new(inner: S, buffer_size: usize, flush_interval: Duration) -> Self {
        Self {
            inner,
            buffer: Vec::with_capacity(buffer_size),
            buffer_size,
            flush_interval,
            last_flush: Instant::now(),
        }
    }

    /// Force flush the buffer
    pub async fn force_flush(&mut self) -> StreamResult<()> {
        for record in self.buffer.drain(..) {
            self.inner.write(record).await?;
        }
        self.inner.flush().await?;
        self.last_flush = Instant::now();
        Ok(())
    }
}

#[async_trait]
impl<T: Send, S: Sink<T> + Send> Sink<T> for BufferedSink<T, S> {
    async fn init(&mut self) -> StreamResult<()> {
        self.inner.init().await
    }

    async fn write(&mut self, record: Record<T>) -> StreamResult<()> {
        self.buffer.push(record);

        let should_flush = self.buffer.len() >= self.buffer_size
            || self.last_flush.elapsed() >= self.flush_interval;

        if should_flush {
            self.force_flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<()> {
        self.force_flush().await
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.force_flush().await?;
        self.inner.close().await
    }
}
