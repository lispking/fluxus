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
        let mut record = unsafe {
            // Safe because we have exclusive access through &mut self
            let source = &mut *(Arc::as_ptr(&inner) as *mut InnerSource<T>);
            source.next().await?
        };

        // Process through existing operators first
        if let Some(mut rec) = record {
            for op in &self.operators {
                let operator = Arc::clone(op);
                let results = unsafe {
                    let op = &mut *(Arc::as_ptr(&operator) as *mut InnerOperator<T, T>);
                    op.process(rec).await?
                };

                if results.is_empty() {
                    return self.next().await;
                }
                rec = results[0].clone();
            }
            record = Some(rec);
        };

        Ok(match record {
            Some(rec) => unsafe {
                let op = &mut *(Arc::as_ptr(&self.operator) as *mut InnerOperator<T, R>);
                op.process(rec).await?.into_iter().next()
            },
            None => None,
        })
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
