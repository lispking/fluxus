use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;

use crate::{InnerOperator, InnerSource};

#[derive(Clone)]
pub struct TransformBase<T: Clone> {
    inner: Arc<InnerSource<T>>,
    operators: Vec<Arc<InnerOperator<T, T>>>,
}

impl<T: Clone + Send + Sync + 'static> TransformBase<T> {
    pub fn new(inner: Arc<InnerSource<T>>) -> Self {
        Self {
            inner,
            operators: Vec::new(),
        }
    }

    pub fn set_operators(&mut self, operators: Vec<Arc<InnerOperator<T, T>>>) {
        self.operators = operators;
    }

    pub async fn process_operators(&mut self, record: Record<T>) -> StreamResult<Vec<Record<T>>> {
        let mut records = vec![record];

        for op in &self.operators {
            let mut processed = Vec::new();

            for rec in records {
                let operator = Arc::clone(op);
                let results = unsafe {
                    // Safe because we have exclusive access through &mut self
                    let op = &mut *(Arc::as_ptr(&operator) as *mut InnerOperator<T, T>);
                    op.process(rec).await?
                };

                processed.extend(results);
            }

            if processed.is_empty() {
                return Ok(Vec::new());
            }

            records = processed;
        }

        Ok(records)
    }

    pub async fn get_next_record(&mut self) -> StreamResult<Option<Record<T>>> {
        let inner = Arc::clone(&self.inner);
        unsafe {
            // Safe because we have exclusive access through &mut self
            let source = &mut *(Arc::as_ptr(&inner) as *mut InnerSource<T>);
            source.next().await
        }
    }

    pub async fn close_inner(&mut self) -> StreamResult<()> {
        let inner = Arc::clone(&self.inner);
        unsafe {
            // Safe because we have exclusive access through &mut self
            let source = &mut *(Arc::as_ptr(&inner) as *mut InnerSource<T>);
            source.close().await
        }
    }
}
