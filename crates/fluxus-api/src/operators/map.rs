use async_trait::async_trait;
use fluxus_core::{Operator, Record, StreamResult};
use std::marker::PhantomData;

pub struct MapOperator<T, R, F> {
    f: F,
    _phantom: PhantomData<(T, R)>,
}

impl<T, R, F> MapOperator<T, R, F>
where
    F: Fn(T) -> R,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, R, F> Operator<T, R> for MapOperator<T, R, F>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    F: Fn(T) -> R + Send + Sync,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<R>>> {
        let result = (self.f)(record.data);
        Ok(vec![Record::with_timestamp(result, record.timestamp)])
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
