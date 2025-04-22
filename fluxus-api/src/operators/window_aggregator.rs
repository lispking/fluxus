use async_trait::async_trait;
use fluxus_core::{Operator, Record, StreamResult, WindowConfig, window::WindowType};
use fluxus_runtime::state::KeyedStateBackend;
use std::marker::PhantomData;

pub struct WindowAggregator<T, A, F> {
    window_config: WindowConfig,
    init: A,
    f: F,
    state: KeyedStateBackend<u64, A>,
    _phantom: PhantomData<T>,
}

impl<T, A, F> WindowAggregator<T, A, F>
where
    A: Clone,
    F: Fn(A, T) -> A,
{
    pub fn new(window_config: WindowConfig, init: A, f: F) -> Self {
        Self {
            window_config,
            init,
            f,
            state: KeyedStateBackend::new(),
            _phantom: PhantomData,
        }
    }

    fn get_window_keys(&self, timestamp: i64) -> Vec<u64> {
        match self.window_config.window_type {
            WindowType::Tumbling(size) => {
                let size_ms = size.as_millis() as i64;
                vec![(timestamp / size_ms) as u64]
            }
            WindowType::Sliding(size, slide) => {
                let size_ms = size.as_millis() as i64;
                let slide_ms = slide.as_millis() as i64;
                let earliest_window = ((timestamp - size_ms) / slide_ms) * slide_ms;
                let latest_window = (timestamp / slide_ms) * slide_ms;

                (earliest_window..=latest_window)
                    .step_by(slide.as_millis() as usize)
                    .filter(|&start| timestamp - start < size_ms)
                    .map(|ts| ts as u64)
                    .collect()
            }
            WindowType::Session(_) => vec![timestamp as u64],
        }
    }
}

#[async_trait]
impl<T, A, F> Operator<T, A> for WindowAggregator<T, A, F>
where
    T: Clone + Send + Sync + 'static,
    A: Clone + Send + Sync + 'static,
    F: Fn(A, T) -> A + Send + Sync,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<A>>> {
        let mut results = Vec::new();

        for window_key in self.get_window_keys(record.timestamp) {
            let current = self
                .state
                .get(&window_key)
                .unwrap_or_else(|| self.init.clone());
            let new_value = (self.f)(current, record.data.clone());
            self.state.set(window_key, new_value.clone());

            results.push(Record {
                data: new_value,
                timestamp: record.timestamp,
            });
        }

        Ok(results)
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
