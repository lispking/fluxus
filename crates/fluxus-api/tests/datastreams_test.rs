use fluxus_api::{CollectionSink, CollectionSource, DataStream};

#[test]
fn test_limit() {
    tokio_test::block_on(async {
        let numbers = vec![1, 2, 3, 4, 5];
        let source = CollectionSource::new(numbers);
        let sink = CollectionSink::new();

        DataStream::new(source)
            .limit(2)
            .sink(sink.clone())
            .await
            .unwrap();

        let data = sink.get_data();
        assert_eq!(data, vec![1, 2]);
    })
}

#[test]
fn test_windowed_limit() {
    tokio_test::block_on(async {
        let numbers = vec![1, 2, 3, 4, 5];
        let source = CollectionSource::new(numbers);
        let sink = CollectionSink::new();

        DataStream::new(source)
            .window(fluxus_utils::window::WindowConfig::global())
            .limit(3)
            .sink(sink.clone())
            .await
            .unwrap();

        let data = sink.get_data();
        assert_eq!(
            data,
            vec![
                vec![1],
                vec![1, 2],
                vec![1, 2, 3],
                vec![1, 2, 3],
                vec![1, 2, 3],
            ]
        );
    })
}

#[test]
fn test_tail() {
    tokio_test::block_on(async {
        let numbers = vec![1, 2, 3, 4, 5, 6];
        let source = CollectionSource::new(numbers);
        let sink = CollectionSink::new();
        DataStream::new(source)
            .window(fluxus_utils::window::WindowConfig::global())
            .tail(3)
            .sink(sink.clone())
            .await
            .unwrap();

        let data = sink.get_data();
        assert_eq!(
            data,
            vec![
                vec![1],
                vec![1, 2],
                vec![1, 2, 3],
                vec![2, 3, 4],
                vec![3, 4, 5],
                vec![4, 5, 6],
            ]
        );
    })
}

#[test]
fn test_flatten() {
    tokio_test::block_on(async {
        let numbers: Vec<Vec<i32>> = vec![vec![1, 2], vec![3, 4, 5]];
        let source = CollectionSource::new(numbers);
        let sink = CollectionSink::new();

        DataStream::new(source)
            .flatten()
            .sink(sink.clone())
            .await
            .unwrap();

        let data = sink.get_data();
        assert_eq!(data, vec![1, 2, 3, 4, 5]);
    })
}

#[test]
fn test_flat_map() {
    tokio_test::block_on(async {
        let numbers: Vec<usize> = vec![1, 2, 3];
        let source = CollectionSource::new(numbers);
        let sink = CollectionSink::new();

        DataStream::new(source)
            .flat_map(|v| vec![v; v])
            .sink(sink.clone())
            .await
            .unwrap();

        let data = sink.get_data();
        assert_eq!(data, vec![1, 2, 2, 3, 3, 3]);
    })
}
