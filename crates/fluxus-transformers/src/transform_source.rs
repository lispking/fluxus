use async_trait::async_trait;
use fluxus_sources::Source;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;

use crate::{InnerOperator, InnerSource};

#[derive(Clone)]
pub struct TransformSource<T: Clone> {
    inner: Arc<InnerSource<T>>,
    operators: Vec<Arc<InnerOperator<T, T>>>,
    buffer: Vec<Record<T>>,
}

impl<T: Clone + Send + Sync + 'static> TransformSource<T> {
    pub fn new(inner: Arc<InnerSource<T>>) -> Self {
        Self {
            inner,
            operators: Vec::new(),
            buffer: Vec::new(),
        }
    }

    pub fn set_operators(&mut self, operators: Vec<Arc<InnerOperator<T, T>>>) {
        self.operators = operators;
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
            return Ok(Some(self.buffer.pop().unwrap()));
        }

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

        for op in &self.operators {
            // Process each record through the current operator and collect all results
            let mut current_op_results = Vec::new();

            for rec in records {
                let operator = Arc::clone(op);
                let results = unsafe {
                    // Safe because we have exclusive access through &mut self
                    let op = &mut *(Arc::as_ptr(&operator) as *mut InnerOperator<T, T>);
                    op.process(rec).await?
                };

                current_op_results.extend(results);
            }

            if current_op_results.is_empty() {
                return self.next().await;
            }

            records = current_op_results;
        }

        self.buffer = records;
        self.buffer.reverse();

        Ok(self.buffer.pop())
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
