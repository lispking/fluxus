use async_trait::async_trait;
use fluxus_transformers::Operator;
use fluxus_utils::models::{Record, StreamResult};
use std::marker::PhantomData;

pub struct FlatMapOperator<T, R, F, I>
where
    I: IntoIterator<Item = R>,
    F: Fn(T) -> I,
{
    f: F,
    _phantom: PhantomData<(T, R)>,
}

impl<T, R, F, I> FlatMapOperator<T, R, F, I>
where
    I: IntoIterator<Item = R>,
    F: Fn(T) -> I,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, R, F, I> Operator<T, R> for FlatMapOperator<T, R, F, I>
where
    T: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    F: Fn(T) -> I + Send + Sync,
    I: IntoIterator<Item = R>,
{
    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<R>>> {
        let Record { data, timestamp } = record;
        let result = (self.f)(data);
        Ok(result
            .into_iter()
            .map(|r| Record::with_timestamp(r, timestamp))
            .collect())
    }
}
