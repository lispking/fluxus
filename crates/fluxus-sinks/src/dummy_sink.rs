use crate::Sink;
use fluxus_utils::models::Record;
use fluxus_utils::models::StreamResult;
use std::marker::PhantomData;

/// A dummy sink that discards all records
#[derive(Default)]
pub struct DummySink<T> {
    _phantom: PhantomData<T>,
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
