use async_trait::async_trait;
use fluxus_sinks::Sink;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::{Arc, Mutex};

/// A sink that collects elements into a Vec
#[derive(Default, Clone)]
pub struct CollectionSink<T> {
    data: Arc<Mutex<Vec<T>>>,
}

impl<T> CollectionSink<T> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_data(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.data
            .lock()
            .map_or_else(|p| p.into_inner().clone(), |d| d.clone())
    }

    pub fn get_last_element(&self) -> Option<T>
    where
        T: Clone,
    {
        self.data
            .lock()
            .map_or_else(|p| p.into_inner().last().cloned(), |d| d.last().cloned())
    }
}

#[async_trait]
impl<T> Sink<T> for CollectionSink<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn write(&mut self, record: Record<T>) -> StreamResult<()> {
        if let Ok(mut data) = self.data.lock() {
            data.push(record.data)
        }
        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
