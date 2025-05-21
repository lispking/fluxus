use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use fluxus_core::ParallelConfig;
use fluxus_runtime::RuntimeContext;
use fluxus_sinks::Sink;
use fluxus_sources::Source;
use fluxus_transformers::Operator;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;
use tokio::sync::Mutex;

// Dummy Source for benchmarking
pub struct DummySource {
    data: Vec<i32>,
    index: usize,
}

#[async_trait]
impl Source<i32> for DummySource {
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> StreamResult<Option<Record<i32>>> {
        if self.index < self.data.len() {
            let record = Record::new(self.data[self.index]);
            self.index += 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}

// Dummy Operator for benchmarking
pub struct DummyOperator;

#[async_trait]
impl Operator<i32, i32> for DummyOperator {
    async fn process(&mut self, record: Record<i32>) -> StreamResult<Vec<Record<i32>>> {
        Ok(vec![record])
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}

// Dummy Sink for benchmarking
pub struct DummySink;

#[async_trait]
impl Sink<i32> for DummySink {
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }
    async fn write(&mut self, _record: Record<i32>) -> StreamResult<()> {
        Ok(())
    }
    async fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }
    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let data_size = 10_000;
    let data: Vec<i32> = (0..data_size).collect();

    c.bench_function("pipeline_execution", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let parallel_config = ParallelConfig::default();
                let runtime_context = RuntimeContext::new(parallel_config);

                let source = DummySource {
                    data: data.clone(),
                    index: 0,
                };
                let operators: Vec<Arc<Mutex<dyn Operator<i32, i32> + Send + Sync>>> = vec![
                    Arc::new(Mutex::new(DummyOperator)),
                    Arc::new(Mutex::new(DummyOperator)),
                ];
                let sink = DummySink;

                runtime_context
                    .execute_pipeline(source, operators, sink)
                    .await
                    .unwrap();
            })
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
