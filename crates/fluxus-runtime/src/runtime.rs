use dashmap::DashMap;
use fluxus_core::ParallelConfig;
use fluxus_sinks::Sink;
use fluxus_sources::Source;
use fluxus_transformers::Operator;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Runtime context for managing stream processing execution
pub struct RuntimeContext {
    /// Task parallelism configuration
    parallel_config: ParallelConfig,
    /// Active task handles
    task_handles: Arc<DashMap<String, Vec<JoinHandle<()>>>>,
}

impl RuntimeContext {
    pub fn new(parallel_config: ParallelConfig) -> Self {
        Self {
            parallel_config,
            task_handles: Arc::new(DashMap::new()),
        }
    }

    /// Execute a source-to-sink pipeline with operators
    pub async fn execute_pipeline<T, S, K>(
        &self,
        source: S,
        operators: Vec<Arc<Mutex<dyn Operator<T, T> + Send + Sync>>>,
        sink: K,
    ) -> StreamResult<()>
    where
        T: Clone + Send + Sync + 'static,
        S: Source<T> + Send + Sync + 'static,
        K: Sink<T> + Send + Sync + 'static,
    {
        let (tx, rx) = mpsc::channel(self.parallel_config.buffer_size);
        let source = Arc::new(Mutex::new(source));
        let sink = Arc::new(Mutex::new(sink));

        // Spawn source task
        let source_handle = self.spawn_source_task(source.clone(), tx.clone());

        // Create channels for operator pipeline
        let mut curr_rx = rx;
        let mut handles = vec![source_handle];

        // Spawn operator tasks
        for operator in operators {
            let (new_tx, new_rx) = mpsc::channel(self.parallel_config.buffer_size);
            let operator_handles = self.spawn_operator_tasks(operator, curr_rx, new_tx);
            handles.extend(operator_handles);
            curr_rx = new_rx;
        }

        // Spawn sink task
        let sink_handle = self.spawn_sink_task(sink.clone(), curr_rx);
        handles.push(sink_handle);

        // Store handles
        self.task_handles
            .insert(Uuid::new_v4().to_string(), handles);

        Ok(())
    }

    fn spawn_source_task<T, S>(
        &self,
        source: Arc<Mutex<S>>,
        tx: mpsc::Sender<Record<T>>,
    ) -> JoinHandle<()>
    where
        T: Clone + Send + 'static,
        S: Source<T> + Send + 'static,
    {
        tokio::spawn(async move {
            loop {
                let mut source_guard = source.lock().await;
                match source_guard.next().await {
                    Ok(Some(record)) => {
                        if tx.send(record).await.is_err() {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            let mut source_guard = source.lock().await;
            let _ = source_guard.close().await;
        })
    }

    fn spawn_operator_tasks<T>(
        &self,
        operator: Arc<Mutex<dyn Operator<T, T> + Send + Sync>>,
        rx: mpsc::Receiver<Record<T>>,
        tx: mpsc::Sender<Record<T>>,
    ) -> Vec<JoinHandle<()>>
    where
        T: Clone + Send + 'static,
    {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..self.parallel_config.parallelism {
            let operator = Arc::clone(&operator);
            let rx = Arc::clone(&rx);
            let tx = tx.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let record = {
                        let mut rx = rx.lock().await;
                        match rx.recv().await {
                            Some(r) => r,
                            None => break,
                        }
                    };

                    let mut op = operator.lock().await;
                    if let Ok(results) = op.process(record).await {
                        for result in results {
                            if tx.send(result).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }

        handles
    }

    fn spawn_sink_task<T, K>(
        &self,
        sink: Arc<Mutex<K>>,
        mut rx: mpsc::Receiver<Record<T>>,
    ) -> JoinHandle<()>
    where
        T: Clone + Send + 'static,
        K: Sink<T> + Send + 'static,
    {
        tokio::spawn(async move {
            while let Some(record) = rx.recv().await {
                let mut sink_guard = sink.lock().await;
                let _ = sink_guard.write(record).await;
            }
            let mut sink_guard = sink.lock().await;
            let _ = sink_guard.flush().await;
            let _ = sink_guard.close().await;
        })
    }
}
