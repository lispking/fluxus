use async_trait::async_trait;
use fluxus_sources::Source;
use fluxus_utils::models::{Record, StreamResult};
use fluxus_utils::time::current_time;
use std::collections::VecDeque;

/// A source that produces elements from a collection
pub struct CollectionSource<T> {
    data: VecDeque<T>,
}

impl<T> CollectionSource<T> {
    pub fn new(data: impl IntoIterator<Item = T>) -> Self {
        Self {
            data: data.into_iter().collect(),
        }
    }
}

#[async_trait]
impl<T> Source<T> for CollectionSource<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
        let value = self.data.pop_front();
        Ok(value.map(|data| Record {
            data,
            timestamp: current_time() as i64,
        }))
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
