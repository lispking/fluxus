//! Fluxus API - High-level interface for stream processing
//! 
//! This module provides a user-friendly API for building stream processing applications.

use std::{sync::Arc, time::Duration, collections::VecDeque};
use async_trait::async_trait;
use fluxus_core::{
    Record, StreamResult, Source, Sink, Operator,
    ParallelConfig, WindowConfig, StreamError,
};
use fluxus_runtime::state::KeyedStateBackend;
use std::sync::Mutex;

/// DataStream represents a stream of data elements
pub struct DataStream<T> {
    source: Arc<dyn Source<T> + Send + Sync>,
    operators: Vec<Arc<dyn Operator<T, T> + Send + Sync>>,
    parallel_config: Option<ParallelConfig>,
}

impl<T> DataStream<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Create a new DataStream from a source
    pub fn new<S>(source: S) -> Self
    where
        S: Source<T> + Send + Sync + 'static,
    {
        Self {
            source: Arc::new(source),
            operators: Vec::new(),
            parallel_config: None,
        }
    }

    /// Set parallelism for the stream processing
    pub fn parallel(mut self, parallelism: usize) -> Self {
        self.parallel_config = Some(ParallelConfig {
            parallelism,
            buffer_size: 1024,
        });
        self
    }

    /// Apply a map transformation
    pub fn map<F, R>(self, f: F) -> DataStream<R>
    where
        F: Fn(T) -> R + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
    {
        let mapper = MapOperator::new(f);
        self.transform(mapper)
    }

    /// Apply a filter transformation
    pub fn filter<F>(mut self, f: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let filter = FilterOperator::new(f);
        self.operators.push(Arc::new(filter));
        self
    }

    /// Transform the stream using a custom operator
    pub fn transform<O, R>(self, operator: O) -> DataStream<R>
    where
        O: Operator<T, R> + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
    {
        let source = TransformSourceLegacy::new(self.source, operator);
        DataStream {
            source: Arc::new(source),
            operators: Vec::new(),
            parallel_config: self.parallel_config,
        }
    }

    /// Apply windowing to the stream
    pub fn window(self, config: WindowConfig) -> WindowedStream<T> {
        WindowedStream {
            stream: self,
            window_config: config,
        }
    }

    /// Write the stream to a sink
    pub async fn sink<K>(self, mut sink: K) -> StreamResult<()>
    where
        K: Sink<T> + Send + Sync + 'static,
    {
        let mut source = TransformSource::new(self.source);
        
        // Add operators to the source's buffer handler
        source.set_operators(self.operators);
        
        // Process all records through the pipeline
        while let Some(record) = source.next().await? {
            sink.write(record).await?;
        }
        
        sink.flush().await?;
        sink.close().await
    }
}

/// Represents a windowed stream for aggregation operations
pub struct WindowedStream<T> {
    stream: DataStream<T>,
    window_config: WindowConfig,
}

impl<T> WindowedStream<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Aggregate values in the window
    pub fn aggregate<A, F>(self, init: A, f: F) -> DataStream<A>
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, T) -> A + Send + Sync + 'static,
    {
        let aggregator = WindowAggregator::new(self.window_config, init, f);
        self.stream.transform(aggregator)
    }
}

/// Built-in operators

struct MapOperator<T, R, F> {
    f: F,
    _phantom: std::marker::PhantomData<(T, R)>,
}

impl<T, R, F> MapOperator<T, R, F>
where
    F: Fn(T) -> R,
{
    fn new(f: F) -> Self {
        Self {
            f,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T, R, F> Operator<T, R> for MapOperator<T, R, F>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    F: Fn(T) -> R + Send + Sync,
{
    async fn process(&self, record: Record<T>) -> StreamResult<Vec<Record<R>>> {
        let result = (self.f)(record.value);
        Ok(vec![Record {
            timestamp: record.timestamp,
            value: result,
            watermark: record.watermark,
        }])
    }

    async fn on_watermark(&self, _watermark: std::time::SystemTime) -> StreamResult<()> {
        Ok(())
    }
}

struct FilterOperator<T, F> {
    f: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> FilterOperator<T, F>
where
    F: Fn(&T) -> bool,
{
    fn new(f: F) -> Self {
        Self {
            f,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T, F> Operator<T, T> for FilterOperator<T, F>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(&T) -> bool + Send + Sync,
{
    async fn process(&self, record: Record<T>) -> StreamResult<Vec<Record<T>>> {
        if (self.f)(&record.value) {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }

    async fn on_watermark(&self, _watermark: std::time::SystemTime) -> StreamResult<()> {
        Ok(())
    }
}

struct WindowAggregator<T, A, F> {
    window_config: WindowConfig,
    init: A,
    f: F,
    state: KeyedStateBackend<u64, A>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, A, F> WindowAggregator<T, A, F>
where
    A: Clone,
    F: Fn(A, T) -> A,
{
    fn new(window_config: WindowConfig, init: A, f: F) -> Self {
        Self {
            window_config,
            init,
            f,
            state: KeyedStateBackend::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    fn get_window_key(&self, timestamp: std::time::SystemTime) -> u64 {
        match self.window_config {
            WindowConfig::Tumbling { size_ms } => {
                let ts = timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as u64;
                ts / size_ms
            }
            WindowConfig::Sliding { size_ms, .. } => {
                let ts = timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as u64;
                ts / size_ms
            }
            WindowConfig::Session { .. } => {
                let ts = timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as u64;
                ts
            }
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
    async fn process(&self, record: Record<T>) -> StreamResult<Vec<Record<A>>> {
        let window_key = self.get_window_key(record.timestamp);
        let current = self.state.get(&window_key).unwrap_or_else(|| self.init.clone());
        let new_value = (self.f)(current, record.value);
        self.state.set(window_key, new_value.clone());
        
        Ok(vec![Record {
            timestamp: record.timestamp,
            value: new_value,
            watermark: record.watermark,
        }])
    }

    async fn on_watermark(&self, _watermark: std::time::SystemTime) -> StreamResult<()> {
        Ok(())
    }
}

/// A source wrapper that helps with transformation
#[derive(Clone)]
struct TransformSource<T: Clone> {
    inner: Arc<dyn Source<T> + Send + Sync>,
    buffer: VecDeque<Record<T>>,
    operators: Vec<Arc<dyn Operator<T, T> + Send + Sync>>,
}

impl<T: Clone + Send + Sync + 'static> TransformSource<T> {
    fn new(inner: Arc<dyn Source<T> + Send + Sync>) -> Self {
        Self {
            inner,
            buffer: VecDeque::new(),
            operators: Vec::new(),
        }
    }

    fn set_operators(&mut self, operators: Vec<Arc<dyn Operator<T, T> + Send + Sync>>) {
        self.operators = operators;
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> Source<T> for TransformSource<T> {
    async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
        // First check buffer for any pending records
        if let Some(record) = self.buffer.pop_front() {
            return Ok(Some(record));
        }

        // Get next record from inner source
        let source = Arc::get_mut(&mut self.inner)
            .ok_or_else(|| StreamError::Processing("Cannot get mutable reference to source".into()))?;
            
        if let Some(record) = source.next().await? {
            let mut current_records = vec![record];
            
            // Process through operator chain
            for operator in &self.operators {
                let mut next_records = Vec::new();
                for record in current_records {
                    let mut results = operator.process(record).await?;
                    next_records.append(&mut results);
                }
                current_records = next_records;
            }
            
            // Split records between first and rest
            if current_records.is_empty() {
                return Ok(None);
            }
            
            let first = current_records.remove(0);
            if !current_records.is_empty() {
                self.buffer.extend(current_records);
            }
            
            return Ok(Some(first));
        }
        
        Ok(None)
    }

    async fn close(&mut self) -> StreamResult<()> {
        match Arc::get_mut(&mut self.inner) {
            Some(source) => source.close().await,
            None => Err(StreamError::Processing("Cannot get mutable reference to source".into())),
        }
    }
}

/// A source that transforms data from an upstream source using an operator
struct TransformSourceLegacy<T, R> {
    source: Arc<dyn Source<T> + Send + Sync>,
    operator: Arc<dyn Operator<T, R> + Send + Sync>,
}

impl<T, R> TransformSourceLegacy<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    fn new<O>(source: Arc<dyn Source<T> + Send + Sync>, operator: O) -> Self
    where
        O: Operator<T, R> + Send + Sync + 'static,
    {
        Self {
            source,
            operator: Arc::new(operator),
        }
    }
}

#[async_trait]
impl<T, R> Source<R> for TransformSourceLegacy<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    async fn next(&mut self) -> StreamResult<Option<Record<R>>> {
        let source = Arc::get_mut(&mut self.source)
            .ok_or_else(|| StreamError::Processing("Cannot get mutable reference to source".into()))?;

        if let Ok(Some(record)) = source.next().await {
            let results = self.operator.process(record).await?;
            if !results.is_empty() {
                return Ok(Some(results[0].clone()));
            }
        }
        Ok(None)
    }

    async fn close(&mut self) -> StreamResult<()> {
        let source = Arc::get_mut(&mut self.source)
            .ok_or_else(|| StreamError::Processing("Cannot get mutable reference to source".into()))?;
        source.close().await
    }
}

/// Built-in sources and sinks
pub mod io {
    use super::*;
    use std::collections::VecDeque;

    /// A source that produces elements from a collection
    pub struct CollectionSource<T> {
        data: VecDeque<T>,
    }

    impl<T> CollectionSource<T> {
        pub fn new(data: impl IntoIterator<Item = T>) -> Self {
            Self {
                data: data.into_iter().collect(),
            }
        }
    }

    #[async_trait]
    impl<T> Source<T> for CollectionSource<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
            Ok(self.data.pop_front().map(|value| Record {
                timestamp: std::time::SystemTime::now(),
                value,
                watermark: None,
            }))
        }

        async fn close(&mut self) -> StreamResult<()> {
            Ok(())
        }
    }

    /// A sink that collects elements into a Vec
    #[derive(Clone)]
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
            self.data.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl<T> Sink<T> for CollectionSink<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        async fn write(&mut self, record: Record<T>) -> StreamResult<()> {
            let mut data = self.data.lock().unwrap();
            data.push(record.value);
            Ok(())
        }

        async fn flush(&mut self) -> StreamResult<()> {
            Ok(())
        }

        async fn close(&mut self) -> StreamResult<()> {
            Ok(())
        }
    }
}
