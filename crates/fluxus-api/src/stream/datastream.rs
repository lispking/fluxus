use crate::operators::{FilterOperator, FlatMapOperator, MapOperator};
use fluxus_core::ParallelConfig;
use fluxus_sinks::Sink;
use fluxus_sources::Source;
use fluxus_transformers::{
    InnerOperator, InnerSource, Operator, TransformSource, TransformSourceWithOperator,
};
use fluxus_utils::{
    models::{StreamError, StreamResult},
    window::WindowConfig,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::WindowedStream;

/// DataStream represents a stream of data elements
pub struct DataStream<T> {
    pub(crate) source: Arc<InnerSource<T>>,
    pub(crate) operators: Vec<Arc<InnerOperator<T, T>>>,
    pub(crate) parallel_config: Option<ParallelConfig>,
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
            preserve_order: true,
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

    /// Apply a flat map transformation
    pub fn flat_map<F, R, I>(self, f: F) -> DataStream<R>
    where
        F: Fn(T) -> I + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        I: IntoIterator<Item = R> + Send + Sync + 'static,
    {
        self.transform(FlatMapOperator::new(f))
    }

    /// Apply a limit transformation that keeps the first n elements
    pub fn limit(self, n: usize) -> Self {
        let n = AtomicUsize::new(n);
        self.filter(move |_| {
            if n.load(Ordering::SeqCst) > 0 {
                n.fetch_sub(1, Ordering::SeqCst);
                true
            } else {
                false
            }
        })
    }

    /// Transform the stream using a custom operator
    pub fn transform<O, R>(self, operator: O) -> DataStream<R>
    where
        O: Operator<T, R> + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
    {
        let source = TransformSourceWithOperator::new(self.source, operator, self.operators);
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
        source.set_operators(self.operators);

        loop {
            match source.next().await {
                Ok(Some(record)) => sink.write(record).await?,
                Ok(None) => break,
                Err(e) => match e {
                    StreamError::EOF => break,
                    StreamError::Wait(ms) => {
                        tokio::time::sleep(std::time::Duration::from_millis(ms)).await
                    }
                    _ => return Err(e),
                },
            }
        }

        sink.flush().await?;
        sink.close().await
    }
}

impl<T> DataStream<Vec<T>>
where
    T: Clone + Send + Sync + 'static,
{
    /// Flatten the stream
    pub fn flatten(self) -> DataStream<T> {
        self.transform(FlatMapOperator::new(|v| v))
    }
}
