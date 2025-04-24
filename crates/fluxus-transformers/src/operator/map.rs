use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamResult};
use std::marker::PhantomData;

/// Built-in map operator
pub struct MapOperator<In, Out, F>
where
    F: Fn(In) -> Out + Send + Sync,
{
    func: F,
    _phantom_in: PhantomData<In>,
    _phantom_out: PhantomData<Out>,
}

impl<In, Out, F> MapOperator<In, Out, F>
where
    F: Fn(In) -> Out + Send + Sync,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom_in: PhantomData,
            _phantom_out: PhantomData,
        }
    }
}

#[async_trait]
impl<In, Out, F> super::Operator<In, Out> for MapOperator<In, Out, F>
where
    In: Send,
    Out: Send,
    F: Fn(In) -> Out + Send + Sync,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn process(&mut self, record: Record<In>) -> StreamResult<Vec<Record<Out>>> {
        let output = (self.func)(record.data);
        Ok(vec![Record::with_timestamp(output, record.timestamp)])
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
