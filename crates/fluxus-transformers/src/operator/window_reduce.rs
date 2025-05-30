use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamResult};
use fluxus_utils::time::current_time;
use fluxus_utils::window::{WindowConfig, WindowType};
use std::collections::HashMap;
use std::marker::PhantomData;

/// Built-in window reduce operator
pub struct WindowReduceOperator<T, F>
where
    T: Clone,
    F: Fn(T, T) -> T + Send + Sync,
{
    func: F,
    window: WindowConfig,
    buffer: HashMap<i64, Vec<Record<T>>>,
    _phantom: PhantomData<T>,
}

impl<T, F> WindowReduceOperator<T, F>
where
    T: Clone,
    F: Fn(T, T) -> T + Send + Sync,
{
    pub fn new(func: F, window: WindowConfig) -> Self {
        Self {
            func,
            window,
            buffer: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    fn get_affected_windows(&self, timestamp: i64) -> Vec<i64> {
        self.window.window_type.get_affected_windows(timestamp)
    }

    fn process_window(&self, records: &[Record<T>]) -> Option<Record<T>> {
        records.first().map(|first| {
            let result = records[1..].iter().fold(first.data.clone(), |acc, record| {
                (self.func)(acc, record.data.clone())
            });
            Record {
                data: result,
                timestamp: first.timestamp,
            }
        })
    }
}

#[async_trait]
impl<T, F> super::Operator<T, T> for WindowReduceOperator<T, F>
where
    T: Clone + Send,
    F: Fn(T, T) -> T + Send + Sync,
{
    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<T>>> {
        let mut results = Vec::new();

        // Get all windows that this record belongs to
        let window_keys = self.get_affected_windows(record.timestamp);

        // Add the record to all relevant windows
        for window_key in window_keys {
            let records = self.buffer.entry(window_key).or_default();
            records.push(record.clone());

            // Process each affected window
            let window_records = records.clone();
            if let Some(result) = self.process_window(&window_records) {
                results.push(result);
            }
        }

        Ok(results)
    }

    async fn on_window_trigger(&mut self) -> StreamResult<Vec<Record<T>>> {
        let mut results = Vec::new();
        let now = current_time() as i64;

        // Process and remove expired windows
        let expired_keys: Vec<_> = self
            .buffer
            .keys()
            .filter(|&&key| match &self.window.window_type {
                WindowType::Tumbling(duration) => {
                    key + duration.as_millis() as i64
                        + self.window.allow_lateness.as_millis() as i64
                        <= now
                }
                WindowType::Sliding(size, _) => {
                    key + size.as_millis() as i64 + self.window.allow_lateness.as_millis() as i64
                        <= now
                }
                WindowType::Session(gap) => {
                    key + gap.as_millis() as i64 + self.window.allow_lateness.as_millis() as i64
                        <= now
                }
                WindowType::Global => {
                    // Global window doesn't expire based on time, so it's never considered expired here
                    false
                }
            })
            .cloned()
            .collect();

        for key in expired_keys {
            if let Some(records) = self.buffer.remove(&key) {
                if let Some(result) = self.process_window(&records) {
                    results.push(result);
                }
            }
        }

        Ok(results)
    }
}
