use async_trait::async_trait;
use fluxus_sources::Source;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;

use crate::{InnerOperator, InnerSource, Operator};

/// A source that applies a single operator transformation
#[derive(Clone)]
pub struct TransformSourceWithOperator<T, R>
where
    T: Clone,
    R: Clone,
{
    inner: Arc<InnerSource<T>>,
    operator: Arc<InnerOperator<T, R>>,
    operators: Vec<Arc<InnerOperator<T, T>>>,
}

impl<T, R> TransformSourceWithOperator<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    pub fn new<O>(
        inner: Arc<InnerSource<T>>,
        operator: O,
        operators: Vec<Arc<InnerOperator<T, T>>>,
    ) -> Self
    where
        O: Operator<T, R> + Send + Sync + 'static,
    {
        Self {
            inner,
            operator: Arc::new(operator),
            operators,
        }
    }
}

#[async_trait]
impl<T, R> Source<R> for TransformSourceWithOperator<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> StreamResult<Option<Record<R>>> {
        let inner = Arc::clone(&self.inner);
        let record = unsafe {
            // Safe because we have exclusive access through &mut self
            let source = &mut *(Arc::as_ptr(&inner) as *mut InnerSource<T>);
            source.next().await?
        };

        // If there's no next record, return None
        let Some(record) = record else {
            return Ok(None);
        };

        let mut records = vec![record];

        // Process through existing operators first
        for op in &self.operators {
            // Process each record through the current operator and collect all results
            let mut processed = Vec::new();

            for rec in records {
                let operator = Arc::clone(op);
                let results = unsafe {
                    let op = &mut *(Arc::as_ptr(&operator) as *mut InnerOperator<T, T>);
                    op.process(rec).await?
                };

                processed.extend(results);
            }

            if processed.is_empty() {
                return self.next().await;
            }

            records = processed;
        }

        if records.is_empty() {
            return Ok(None);
        }

        let mut final_results = Vec::new();
        for rec in records {
            final_results.extend(unsafe {
                let op = &mut *(Arc::as_ptr(&self.operator) as *mut InnerOperator<T, R>);
                op.process(rec).await?
            });
        }

        Ok(final_results.into_iter().next())
    }

    async fn close(&mut self) -> StreamResult<()> {
        let inner = Arc::clone(&self.inner);
        unsafe {
            // Safe because we have exclusive access through &mut self
            let source = &mut *(Arc::as_ptr(&inner) as *mut InnerSource<T>);
            source.close().await
        }
    }
}
