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
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn process(&mut self, record: Record<T>) -> StreamResult<Vec<Record<T>>> {
        if (self.f)(&record.data) {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{CollectionSink, CollectionSource, DataStream};

    #[test]
    fn test_filter() {
        tokio_test::block_on(async {
            let numbers = vec![1, 2, 3, 4, 5];
            // TransformSourceWithOperator::new();
            let source = CollectionSource::new(numbers);
            let sink = CollectionSink::new();

            DataStream::new(source)
            .filter(|x| x % 2 == 0)
            .sink(sink.clone())
            .await
            .unwrap();

            let data = sink.get_data();
            println!( "data: {:?}", data);
            assert_eq!(data.len(), 2);
            assert_eq!(data[0], 2);
            assert_eq!(data[1], 4);
        })
    }
}