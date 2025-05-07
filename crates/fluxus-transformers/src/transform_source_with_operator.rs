use async_trait::async_trait;
use fluxus_sources::Source;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;

use crate::{InnerOperator, InnerSource, Operator, TransformBase};

/// A source that applies a single operator transformation
#[derive(Clone)]
pub struct TransformSourceWithOperator<T, R>
where
    T: Clone,
    R: Clone,
{
    base: TransformBase<T>,
    operator: Arc<InnerOperator<T, R>>,
    buffer: Vec<Record<R>>,
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
        let mut base = TransformBase::new(inner);
        base.set_operators(operators);
        Self {
            base,
            operator: Arc::new(operator),
            buffer: Vec::new(),
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
        if !self.buffer.is_empty() {
            return Ok(self.buffer.pop());
        }
        let record = self.base.get_next_record().await?;

        // If there's no next record, return None
        let Some(record) = record else {
            return Ok(None);
        };

        let records = self.base.process_operators(record).await?;

        if records.is_empty() {
            return self.next().await;
        }

        let mut final_results = Vec::new();
        for rec in records {
            final_results.extend(unsafe {
                let op = &mut *(Arc::as_ptr(&self.operator) as *mut InnerOperator<T, R>);
                op.process(rec).await?
            });
        }
        self.buffer = final_results;
        self.buffer.reverse();

        Ok(self.buffer.pop())
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.base.close_inner().await
    }
}
