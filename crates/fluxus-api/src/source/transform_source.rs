use async_trait::async_trait;
use fluxus_core::{Record, Source, StreamResult};
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
        let inner = Arc::clone(&self.inner);
        unsafe {
            // Safe because we have exclusive access through &mut self
            let source = &mut *(Arc::as_ptr(&inner) as *mut InnerSource<T>);
            source.next().await
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
