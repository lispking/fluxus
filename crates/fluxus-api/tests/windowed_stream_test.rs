#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use fluxus_api::operators::SortOrder;
    use fluxus_api::{CollectionSink, CollectionSource, DataStream};
    use fluxus_sources::Source;
    use fluxus_utils::models::Record;
    use fluxus_utils::{models::StreamResult, window::WindowConfig};

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

    #[test]
    fn test_distinct() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec!["1", "22", "1", "22", "333", "333"]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .distinct()
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 6);
            assert_eq!(data[5].len(), 3);
            assert!(data[5].contains("1"));
            assert!(data[5].contains("22"));
            assert!(data[5].contains("333"));

            let source = CollectionSource::new(vec!["1", "11", "111", "111"]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .distinct_by_key(|s| s.as_bytes()[0])
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 4);
            assert_eq!(data[3].len(), 1);
            assert!(data[3].contains(&"1"));
        })
    }

    #[test]
    fn test_top_k() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec![1, 2, 3, 4, 5]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .top_k(3)
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 5);
            assert_eq!(data[0], vec![1]);
            assert_eq!(data[1], vec![2, 1]);
            assert_eq!(data[2], vec![3, 2, 1]);
            assert_eq!(data[3], vec![4, 3, 2]);
            assert_eq!(data[4], vec![5, 4, 3]);

            let source = CollectionSource::new(vec!["1", "2", "3", "3", "3"]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .top_k_by_key(3, |s| s.as_bytes()[0])
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            dbg!(&data);
            assert_eq!(data.len(), 5);
            assert_eq!(data[0], vec!["1"]);
            assert_eq!(data[1], vec!["2", "1"]);
            assert_eq!(data[2], vec!["3", "2", "1"]);
            assert_eq!(data[3], vec!["3", "3", "2"]);
            assert_eq!(data[4], vec!["3", "3", "3"]);
        })
    }

    #[test]
    fn test_skip() {
        tokio_test::block_on(async {
            let source = CollectionSource::new(vec![1, 2, 3, 4, 5]);
            let sink = CollectionSink::new();
            DataStream::new(source)
                .window(WindowConfig::global())
                .skip(2)
                .sink(sink.clone())
                .await
                .unwrap();
            let data = sink.get_data();
            assert_eq!(data.len(), 5);
            assert_eq!(data[0], Vec::<i32>::new());
            assert_eq!(data[1], Vec::<i32>::new());
            assert_eq!(data[2], vec![3]);
            assert_eq!(data[3], vec![3, 4]);
            assert_eq!(data[4], vec![3, 4, 5]);
        })
    }
}
