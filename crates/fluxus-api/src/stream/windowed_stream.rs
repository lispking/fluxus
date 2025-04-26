use std::cmp::Ordering;

use fluxus_transformers::operator::{WindowAllOperator, WindowAnyOperator};
use fluxus_utils::window::WindowConfig;

use crate::operators::{SortOrder, WindowAggregator, WindowSorter, WindowTimestampSorter};
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

    /// Sort values in the window
    pub fn sort_by<F>(self, f: F) -> DataStream<Vec<T>>
    where
        F: FnMut(&T, &T) -> Ordering + Send + Sync + 'static,
    {
        let sorter = WindowSorter::new(self.window_config, f);
        self.stream.transform(sorter)
    }

    /// Sort values in the window by timestamp
    pub fn sort_by_ts(self, order: SortOrder) -> DataStream<Vec<T>> {
        let sorter = WindowTimestampSorter::new(self.window_config, order);
        self.stream.transform(sorter)
    }

    /// Sort values in the window by timestamp in ascending order
    pub fn sort_by_ts_asc(self) -> DataStream<Vec<T>> {
        let sorter = WindowTimestampSorter::new(self.window_config, SortOrder::Asc);
        self.stream.transform(sorter)
    }

    /// Sort values in the window by timestamp in descending order
    pub fn sort_by_ts_desc(self) -> DataStream<Vec<T>> {
        let sorter = WindowTimestampSorter::new(self.window_config, SortOrder::Desc);
        self.stream.transform(sorter)
    }
}

impl<T> WindowedStream<T>
where
    T: Ord + Clone + Send + Sync + 'static,
{
    /// Sort values in specified order
    pub fn sort(self, ord: SortOrder) -> DataStream<Vec<T>> {
        self.sort_by(move |v1, v2| match ord {
            SortOrder::Asc => v1.cmp(v2),
            SortOrder::Desc => v2.cmp(v1),
        })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use fluxus_sources::Source;
    use fluxus_utils::models::Record;
    use fluxus_utils::{models::StreamResult, window::WindowConfig};

    use crate::{CollectionSink, CollectionSource, DataStream, operators::SortOrder};

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

    #[test]
    fn test_sort_by() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec!["1", "4444", "55555", "22", "333"]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .sort_by(|a, b| a.len().cmp(&b.len()))
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 5);
            assert_eq!(data[0], vec!["1"]);
            assert_eq!(data[1], vec!["1", "4444"]);
            assert_eq!(data[2], vec!["1", "4444", "55555"]);
            assert_eq!(data[3], vec!["1", "22", "4444", "55555"]);
            assert_eq!(data[4], vec!["1", "22", "333", "4444", "55555"]);
        })
    }

    #[test]
    fn test_sort() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec!["1", "4444", "55555", "22", "333"]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .sort(SortOrder::Asc)
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 5);
            assert_eq!(data[0], vec!["1"]);
            assert_eq!(data[1], vec!["1", "4444"]);
            assert_eq!(data[2], vec!["1", "4444", "55555"]);
            assert_eq!(data[3], vec!["1", "22", "4444", "55555"]);
            assert_eq!(data[4], vec!["1", "22", "333", "4444", "55555"]);
        })
    }

    struct SlowSource<T> {
        inner: CollectionSource<T>,
        counter: i64,
    }
    #[async_trait]
    impl<T> Source<T> for SlowSource<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        async fn init(&mut self) -> StreamResult<()> {
            Ok(())
        }

        async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
            self.inner.next().await.map(|op| {
                op.map(|mut r| {
                    self.counter += 1;
                    r.timestamp += self.counter;
                    r
                })
            })
        }

        async fn close(&mut self) -> StreamResult<()> {
            Ok(())
        }
    }
    #[test]
    fn test_sort_by_ts() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec!["1st", "2nd", "3rd", "4th", "5th"]);
            let source = SlowSource {
                inner: source,
                counter: 0,
            };
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .sort_by_ts(SortOrder::Asc)
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 5);
            assert_eq!(
                data,
                vec![
                    vec!["1st"],
                    vec!["1st", "2nd"],
                    vec!["1st", "2nd", "3rd"],
                    vec!["1st", "2nd", "3rd", "4th"],
                    vec!["1st", "2nd", "3rd", "4th", "5th"],
                ]
            );
            let source = CollectionSource::new(vec!["1st", "2nd", "3rd", "4th", "5th"]);
            let source = SlowSource {
                inner: source,
                counter: 0,
            };
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .sort_by_ts(SortOrder::Desc)
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 5);
            let rev = |mut v: Vec<_>| {
                v.reverse();
                v
            };
            assert_eq!(
                data,
                vec![
                    rev(vec!["1st"]),
                    rev(vec!["1st", "2nd"]),
                    rev(vec!["1st", "2nd", "3rd"]),
                    rev(vec!["1st", "2nd", "3rd", "4th"]),
                    rev(vec!["1st", "2nd", "3rd", "4th", "5th"]),
                ]
            );
        })
    }
}
