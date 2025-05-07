#[cfg(test)]
mod tests {
    use fluxus_api::{CollectionSink, CollectionSource, DataStream};

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
            println!("data: {:?}", data);
            assert_eq!(data.len(), 2);
            assert_eq!(data[0], 2);
            assert_eq!(data[1], 4);
        })
    }
}
