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
}

impl<T, R> TransformSourceWithOperator<T, R>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    pub fn new<O>(inner: Arc<InnerSource<T>>, operator: O) -> Self
    where
        O: Operator<T, R> + Send + Sync + 'static,
    {
        Self {
            inner,
            operator: Arc::new(operator),
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

        match record {
            Some(record) => {
                let operator = Arc::clone(&self.operator);
                let output = unsafe {
                    // Safe because we have exclusive access through &mut self
                    let op = &mut *(Arc::as_ptr(&operator) as *mut InnerOperator<T, R>);
                    op.process(record).await?
                };
                Ok(output.into_iter().next())
            }
            None => Ok(None),
        }
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
