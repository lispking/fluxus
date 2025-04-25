use async_trait::async_trait;
use fluxus_runtime::state::KeyedStateBackend;
use fluxus_transformers::Operator;
use fluxus_utils::{
    models::{Record, StreamResult},
    window::WindowConfig,
};
use std::{cmp::Ordering, marker::PhantomData};

/// sort_by operator for windowed stream.
pub struct WindowSorter<T, F> {
    window_config: WindowConfig,
    f: F,
    state: KeyedStateBackend<u64, Vec<T>>,
    _phantom: PhantomData<T>,
}

impl<T, F> WindowSorter<T, F>
where
    F: FnMut(&T, &T) -> Ordering,
{
    pub fn new(window_config: WindowConfig, f: F) -> Self {
        Self {
            window_config,
            f,
            state: KeyedStateBackend::new(),
            _phantom: PhantomData,
        }
    }

    fn get_window_keys(&self, timestamp: i64) -> Vec<u64> {
        self.window_config.window_type.get_window_keys(timestamp)
    }
}

#[async_trait]
impl<T, F> Operator<T, Vec<T>> for WindowSorter<T, F>
where
    T: Clone + Send + Sync + 'static,
    F: FnMut(&T, &T) -> Ordering + Send + Sync,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<Vec<T>>>> {
        let mut results = Vec::new();

        for window_key in self.get_window_keys(record.timestamp) {
            let mut current = self.state.get(&window_key).unwrap_or_default();
            let index = current
                .binary_search_by(|prob| (self.f)(prob, &record.data))
                .unwrap_or_else(|i| i);
            current.insert(index, record.data.clone());

            self.state.set(window_key, current.clone());
            results.push(Record {
                data: current,
                timestamp: record.timestamp,
            });
        }

        Ok(results)
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
