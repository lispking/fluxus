use async_trait::async_trait;
use fluxus_transformers::Operator;
use fluxus_utils::{
    models::{Record, StreamResult},
    window::WindowConfig,
};
use std::{collections::HashMap, marker::PhantomData};

pub struct WindowSkipper<T> {
    window_config: WindowConfig,
    n: usize,
    buffer: HashMap<u64, Vec<T>>,
    _phantom: PhantomData<T>,
}

impl<T> WindowSkipper<T>
where
    T: Clone,
{
    pub fn new(window_config: WindowConfig, n: usize) -> Self {
        Self {
            window_config,
            n,
            buffer: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    fn get_window_keys(&self, timestamp: i64) -> Vec<u64> {
        self.window_config.window_type.get_window_keys(timestamp)
    }
}

#[async_trait]
impl<T> Operator<T, Vec<T>> for WindowSkipper<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<Vec<T>>>> {
        let mut results = Vec::new();

        for window_key in self.get_window_keys(record.timestamp) {
            let records = self.buffer.entry(window_key).or_default();
            records.push(record.data.clone());
            let new_records = records.iter().skip(self.n).cloned().collect::<Vec<_>>();
            results.push(Record {
                data: new_records,
                timestamp: record.timestamp,
            });
        }

        Ok(results)
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
