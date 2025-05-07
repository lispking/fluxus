use std::{collections::HashMap, marker::PhantomData};

use async_trait::async_trait;
use fluxus_utils::{
    models::{Record, StreamResult},
    window::WindowConfig,
};

use super::Operator;

pub struct WindowAnyOperator<T, F> {
    func: F,
    window: WindowConfig,
    buffer: HashMap<i64, Vec<Record<T>>>,
    _phantom: PhantomData<T>,
}

impl<T, F> WindowAnyOperator<T, F>
where
    T: Clone,
    F: Fn(&T) -> bool + Send + Sync,
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

    fn process_window(&self, records: &[Record<T>]) -> Option<Record<bool>> {
        records.first().map(|first| Record {
            data: records.iter().any(|record| (self.func)(&record.data)),
            timestamp: first.timestamp,
        })
    }
}

#[async_trait]
impl<T, F> Operator<T, bool> for WindowAnyOperator<T, F>
where
    T: Clone + Send + 'static,
    F: Fn(&T) -> bool + Send + Sync,
{
    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<bool>>> {
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
}

pub struct WindowAllOperator<T, F> {
    func: F,
    window: WindowConfig,
    buffer: HashMap<i64, Vec<Record<T>>>,
    _phantom: PhantomData<T>,
}

impl<T, F> WindowAllOperator<T, F>
where
    T: Clone,
    F: Fn(&T) -> bool + Send + Sync,
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

    fn process_window(&self, records: &[Record<T>]) -> Option<Record<bool>> {
        // 由于前面已经检查了records不为空，这里可以安全地使用first()
        records.first().map(|first| Record {
            data: records.iter().all(|record| (self.func)(&record.data)),
            timestamp: first.timestamp,
        })
    }
}

#[async_trait]
impl<T, F> Operator<T, bool> for WindowAllOperator<T, F>
where
    T: Clone + Send + 'static,
    F: Fn(&T) -> bool + Send + Sync,
{
    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<bool>>> {
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
}
