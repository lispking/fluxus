use crate::models::Record;
use crate::models::StreamResult;
use crate::sink::Sink;
use std::marker::PhantomData;

/// A dummy sink that discards all records
pub struct DummySink<T> {
    _phantom: PhantomData<T>,
}

impl<T> Default for DummySink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DummySink<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T: Send> Sink<T> for DummySink<T> {
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn write(&mut self, _record: Record<T>) -> StreamResult<()> {
        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
