use async_trait::async_trait;
use fluxus_sources::Source;
use fluxus_utils::models::{Record, StreamResult};
use std::sync::Arc;

use crate::{InnerOperator, InnerSource};

#[derive(Clone)]
pub struct TransformSource<T: Clone> {
    inner: Arc<InnerSource<T>>,
    operators: Vec<Arc<InnerOperator<T, T>>>,
}

impl<T: Clone + Send + Sync + 'static> TransformSource<T> {
    pub fn new(inner: Arc<InnerSource<T>>) -> Self {
        Self {
            inner,
            operators: Vec::new(),
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
        loop {
            let inner = Arc::clone(&self.inner);
            let record = unsafe {
                // Safe because we have exclusive access through &mut self
                let source = &mut *(Arc::as_ptr(&inner) as *mut InnerSource<T>);
                source.next().await?
            };

            match record {
                Some(record) => {
                    let mut current_record = Some(record);
                    let mut filtered_out = false;

                    // Apply each operator in sequence
                    for op in &self.operators {
                        if let Some(rec) = current_record {
                            let operator = Arc::clone(op);
                            let results = unsafe {
                                // Safe because we have exclusive access through &mut self
                                let op = &mut *(Arc::as_ptr(&operator) as *mut InnerOperator<T, T>);
                                op.process(rec).await?
                            };

                            // If the operator filtered out the record (empty results), mark as filtered
                            if results.is_empty() {
                                filtered_out = true;
                                current_record = None;
                                break;
                            } else {
                                // Otherwise, take the first result for the next operator
                                current_record = Some(results[0].clone());
                            }
                        }
                    }

                    // If the record was filtered out, continue to the next record
                    if filtered_out {
                        continue;
                    }

                    return Ok(current_record);
                }
                None => return Ok(None),
            }
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
