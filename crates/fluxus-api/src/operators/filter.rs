use async_trait::async_trait;
use fluxus_transformers::Operator;
use fluxus_utils::models::{Record, StreamResult};
use std::marker::PhantomData;

pub struct FilterOperator<T, F> {
    f: F,
    _phantom: PhantomData<T>,
}

impl<T, F> FilterOperator<T, F>
where
    F: Fn(&T) -> bool,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, F> Operator<T, T> for FilterOperator<T, F>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(&T) -> bool + Send + Sync,
{
    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<T>>> {
        if (self.f)(&record.data) {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }
}
