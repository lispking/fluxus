use crate::models::{Record, StreamResult};
use crate::window::{WindowConfig, WindowType};
use async_trait::async_trait;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

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
        match &self.window.window_type {
            WindowType::Tumbling(duration) => {
                let duration_ms = duration.as_millis() as i64;
                vec![(timestamp / duration_ms) * duration_ms]
            }
            WindowType::Sliding(size, slide) => {
                let slide_ms = slide.as_millis() as i64;
                let size_ms = size.as_millis() as i64;
                let earliest_window = ((timestamp - size_ms) / slide_ms) * slide_ms;
                let latest_window = (timestamp / slide_ms) * slide_ms;

                (earliest_window..=latest_window)
                    .step_by(slide.as_millis() as usize)
                    .filter(|&start| timestamp - start < size_ms)
                    .collect()
            }
            WindowType::Session(gap) => {
                let gap_ms = gap.as_millis() as i64;
                vec![timestamp / gap_ms]
            }
            WindowType::Global => {
                vec![0] // Global window can be represented by a single key, here we use 0
            }
        }
    }

    fn process_window(&self, records: &[Record<T>]) -> Option<Record<T>> {
        if records.is_empty() {
            return None;
        }

        let mut iter = records.iter();
        let first = iter.next().unwrap();
        let result = iter.fold(first.data.clone(), |acc, record| {
            (self.func)(acc, record.data.clone())
        });

        Some(Record {
            data: result,
            timestamp: first.timestamp,
        })
    }
}

#[async_trait]
impl<T, F> super::Operator<T, T> for WindowReduceOperator<T, F>
where
    T: Clone + Send,
    F: Fn(T, T) -> T + Send + Sync,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

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

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
