use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamResult};
use std::marker::PhantomData;

/// Built-in filter operator
pub struct FilterOperator<T, F>
where
    F: Fn(&T) -> bool + Send + Sync,
{
    func: F,
    _phantom: PhantomData<T>,
}

impl<T, F> FilterOperator<T, F>
where
    F: Fn(&T) -> bool + Send + Sync,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, F> super::Operator<T, T> for FilterOperator<T, F>
where
    T: Send,
    F: Fn(&T) -> bool + Send + Sync,
{
    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<T>>> {
        if (self.func)(&record.data) {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }
}
