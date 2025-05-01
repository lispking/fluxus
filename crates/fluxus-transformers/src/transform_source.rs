use async_trait::async_trait;
use fluxus_sources::Source;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;

use crate::{InnerOperator, InnerSource, TransformBase};

#[derive(Clone)]
pub struct TransformSource<T: Clone> {
    base: TransformBase<T>,
    buffer: Vec<Record<T>>,
}

impl<T: Clone + Send + Sync + 'static> TransformSource<T> {
    pub fn new(inner: Arc<InnerSource<T>>) -> Self {
        Self {
            base: TransformBase::new(inner),
            buffer: Vec::new(),
        }
    }

    pub fn set_operators(&mut self, operators: Vec<Arc<InnerOperator<T, T>>>) {
        self.base.set_operators(operators);
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> Source<T> for TransformSource<T> {
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
        // If we have records in the buffer, return one
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

        self.buffer = records;
        self.buffer.reverse();

        Ok(self.buffer.pop())
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.base.close_inner().await
    }
}
