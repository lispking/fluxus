use super::status::PipelineStatus;
use crate::BackpressureStrategy;
use crate::Counter;
use crate::ParallelConfig;
use crate::RetryStrategy;
use crate::Timer;
use crate::error_handling::BackpressureController;
use crate::error_handling::ErrorHandler;
use crate::metrics::Metrics;
use fluxus_sinks::Sink;
use fluxus_sinks::dummy_sink::DummySink;
use fluxus_sources::Source;
use fluxus_transformers::operator::Operator;
use fluxus_utils::models::Record;
use fluxus_utils::models::StreamResult;
use fluxus_utils::window::WindowConfig;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::time;
use tracing;

/// Represents a stream processing pipeline
pub struct Pipeline<T: Clone> {
    /// The data source
    source: Box<dyn Source<T>>,
    /// The sequence of operators
    operators: Vec<Box<dyn Operator<T, T>>>,
    /// The data sink
    sink: Box<dyn Sink<T>>,
    /// Window configuration (optional)
    window_config: Option<WindowConfig>,
    /// Parallel processing configuration
    parallel_config: ParallelConfig,
    /// Current pipeline status
    status: PipelineStatus,
    /// Last watermark timestamp
    last_watermark: i64,
    /// Metrics tracking
    metrics: Arc<Metrics>,
    process_timer: Arc<Timer>,
    records_processed: Arc<Counter>,
    records_failed: Arc<Counter>,
    /// Error handling
    error_handler: ErrorHandler,
    /// Backpressure controller
    backpressure: BackpressureController,
}

impl<T: 'static + Send + Clone> Pipeline<T> {
    /// Create a new pipeline with a source
    pub fn source<S: Source<T> + 'static>(source: S) -> Self {
        let mut metrics = Metrics::new();
        let process_timer = metrics.timer("process_time");
        let records_processed = metrics.counter("records_processed");
        let records_failed = metrics.counter("records_failed");

        Self {
            source: Box::new(source),
            operators: Vec::new(),
            sink: Box::new(DummySink::new()),
            window_config: None,
            parallel_config: ParallelConfig::default(),
            status: PipelineStatus::Ready,
            last_watermark: 0,
            metrics: Arc::new(metrics),
            process_timer,
            records_processed,
            records_failed,
            error_handler: ErrorHandler::new(RetryStrategy::exponential(
                Duration::from_millis(100),
                Duration::from_secs(10),
                3,
                2.0,
            )),
            backpressure: BackpressureController::new(BackpressureStrategy::Throttle {
                high_watermark: 1000,
                low_watermark: 100,
                backoff: Duration::from_millis(50),
            }),
        }
    }

    /// Add an operator to the pipeline
    pub fn add_operator<O: Operator<T, T> + 'static>(mut self, operator: O) -> Self {
        self.operators.push(Box::new(operator));
        self
    }

    /// Set the sink for the pipeline
    pub fn sink<S: Sink<T> + 'static>(mut self, sink: S) -> Self {
        self.sink = Box::new(sink);
        self
    }

    /// Configure windowing for the pipeline
    pub fn window(mut self, config: WindowConfig) -> Self {
        self.window_config = Some(config);
        self
    }

    /// Configure parallel processing for the pipeline
    pub fn parallel(mut self, config: ParallelConfig) -> Self {
        self.parallel_config = config;
        self
    }

    /// Configure error handling strategy
    pub fn with_retry_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.error_handler = ErrorHandler::new(strategy);
        self
    }

    /// Configure backpressure strategy
    pub fn with_backpressure_strategy(mut self, strategy: BackpressureStrategy) -> Self {
        self.backpressure = BackpressureController::new(strategy);
        self
    }

    /// Get current pipeline status
    pub fn status(&self) -> PipelineStatus {
        self.status
    }

    /// Get a snapshot of current metrics
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Update watermark and trigger windows if needed
    async fn process_watermark(&mut self) -> StreamResult<()> {
        if let Some(window_config) = &self.window_config {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System time cannot be earlier than UNIX epoch")
                .as_millis() as i64;

            // Check if we should advance the watermark
            if now - self.last_watermark >= window_config.watermark_delay.as_millis() as i64 {
                self.last_watermark = now;

                // Trigger windows in all operators
                for op in &mut self.operators {
                    let results = op.on_window_trigger().await?;
                    for record in results {
                        self.sink.write(record).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Process a record through a single operator with retries
    async fn process_with_retry(
        error_handler: &ErrorHandler,
        op: &mut Box<dyn Operator<T, T>>,
        record: Record<T>,
    ) -> StreamResult<Vec<Record<T>>> {
        let record = record.clone();
        let op_ref = &mut **op;

        error_handler
            .retry(|| {
                let rt = Handle::current();
                rt.block_on(op_ref.process(record.clone()))
            })
            .await
    }

    /// Write a record to the sink with retries
    async fn write_with_retry(
        error_handler: &ErrorHandler,
        sink: &mut Box<dyn Sink<T>>,
        record: Record<T>,
    ) -> StreamResult<()> {
        let record = record.clone();
        let sink_ref = &mut **sink;

        error_handler
            .retry(|| {
                let rt = Handle::current();
                rt.block_on(sink_ref.write(record.clone()))
            })
            .await
    }

    /// Execute the pipeline with error handling and backpressure
    pub async fn execute(mut self) -> StreamResult<()> {
        self.status = PipelineStatus::Running;

        // Initialize components
        self.source.init().await?;
        for op in &mut self.operators {
            op.init().await?;
        }
        self.sink.init().await?;

        let mut watermark_interval = time::interval(Duration::from_millis(100));

        loop {
            if self.backpressure.should_apply_backpressure() {
                if let Some(backoff) = self.backpressure.get_backoff() {
                    tracing::debug!("Applying backpressure, waiting for {:?}", backoff);
                    time::sleep(backoff).await;
                    continue;
                }
            }

            tokio::select! {
                result = self.source.next() => {
                    match result {
                        Ok(Some(record)) => {
                            let start = Instant::now();
                            let mut records = vec![record];
                            let mut success = true;

                            // Process through operators with retry
                            for op in &mut self.operators {
                                let mut next = Vec::new();
                                let current_records = std::mem::take(&mut records);

                                for record in current_records {
                                    match Self::process_with_retry(&self.error_handler, op, record).await {
                                        Ok(mut results) => next.append(&mut results),
                                        Err(e) => {
                                            self.records_failed.increment();
                                            success = false;
                                            tracing::error!("Operator error after retries: {}", e);
                                            break;
                                        }
                                    }
                                }

                                if !success {
                                    break;
                                }
                                records = next;
                            }

                            // Use the length before consuming records
                            let record_count = records.len();
                            self.backpressure.update_load(record_count);

                            if success {
                                while let Some(record) = records.pop() {
                                    match Self::write_with_retry(&self.error_handler, &mut self.sink, record).await {
                                        Ok(_) => {
                                            self.records_processed.increment();
                                        }
                                        Err(e) => {
                                            self.records_failed.increment();
                                            tracing::error!("Sink error after retries: {}", e);
                                        }
                                    }
                                }
                            }

                            if let Some(timer) = Arc::get_mut(&mut self.process_timer) {
                                timer.record(start.elapsed());
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            self.records_failed.increment();
                            tracing::error!("Source error: {}", e);
                            return Err(e);
                        }
                    }
                }

                _ = watermark_interval.tick() => {
                    if let Err(e) = self.process_watermark().await {
                        tracing::error!("Watermark error: {}", e);
                    }
                }
            }
        }

        self.sink.flush().await?;
        self.sink.close().await?;
        self.status = PipelineStatus::Completed;
        Ok(())
    }
}
