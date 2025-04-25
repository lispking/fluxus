#[cfg(test)]
mod tests {

    #[cfg(feature = "enable-gharchive")]
    use fluxus_sources::Source;
    #[cfg(feature = "enable-gharchive")]
    use fluxus_sources::gharchive;
    #[cfg(feature = "enable-gharchive")]
    use tokio::test;

    #[cfg(feature = "enable-gharchive")]
    #[test]
    async fn test_local_source() {
        let uri = std::env::var("TEST_FILE_NAME").unwrap();

        let mut gh_source = gharchive::GHarchiveSource::new(&uri);
        gh_source.init().await.expect("init failed");

        for n in 0..100 {
            println!("test={:?}", gh_source.next().await.expect("error"));
        }
    }

    #[cfg(feature = "enable-gharchive")]
    #[test]
    async fn test_http_source() {
        let uri = "https://data.gharchive.org/2015-01-01-15.json.gz";
        let mut gh_source_gzip = gharchive::GHarchiveSource::new(uri);
        gh_source_gzip.init().await.expect("init failed");

        for n in 0..100 {
            let mut foo = gh_source_gzip.next().await.expect("error1");
            println!("test={:?}", gh_source_gzip.next().await.expect("error"));
        }
    }
}
