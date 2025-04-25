use fluxus_transformers::operator::{WindowAllOperator, WindowAnyOperator};
use fluxus_utils::window::WindowConfig;

use crate::operators::WindowAggregator;
use crate::stream::datastream::DataStream;

/// Represents a windowed stream for aggregation operations
pub struct WindowedStream<T> {
    pub(crate) stream: DataStream<T>,
    pub(crate) window_config: WindowConfig,
}

impl<T> WindowedStream<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Aggregate values in the window
    pub fn aggregate<A, F>(self, init: A, f: F) -> DataStream<A>
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, T) -> A + Send + Sync + 'static,
    {
        let aggregator = WindowAggregator::new(self.window_config, init, f);
        self.stream.transform(aggregator)
    }

    pub fn any<F>(self, f: F) -> DataStream<bool>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let anyer = WindowAnyOperator::new(f, self.window_config);
        self.stream.transform(anyer)
    }

    pub fn all<F>(self, f: F) -> DataStream<bool>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let aller = WindowAllOperator::new(f, self.window_config);
        self.stream.transform(aller)
    }
}

#[cfg(test)]
mod tests {
    use fluxus_utils::window::WindowConfig;

    use crate::{CollectionSink, CollectionSource, DataStream};

    #[test]
    fn test_any() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec![1, 2, 3, 4, 5]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .any(|x| x % 2 == 0)
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data[0], false);
            assert_eq!(data[1], true);
            assert_eq!(data[2], true);
            assert_eq!(data[3], true);
            assert_eq!(data[4], true);
        })
    }

    #[test]
    fn test_all() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec![1, 2, 3, 4, 5]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .all(|x| x % 2 == 0)
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data[0], false);
            assert_eq!(data[1], false);
            assert_eq!(data[2], false);
            assert_eq!(data[3], false);
            assert_eq!(data[4], false);
        })
    }
}
