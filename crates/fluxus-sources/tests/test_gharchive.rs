#[cfg(test)]
mod tests {
    #[cfg(feature = "gharchive")]
    use {
        fluxus_sources::{Source, gharchive},
        std::io::Write,
        tokio::test,
    };

    #[cfg(feature = "gharchive")]
    #[test]
    async fn test_local_source() {
        use tempfile::NamedTempFile;

        let url = "https://data.gharchive.org/2012-01-01-15.json.gz";
        let response = reqwest::get(url).await.unwrap();

        if response.status().is_success() {
            let body = response.bytes().await.unwrap();
            let mut temp_file = NamedTempFile::with_suffix(".gz").unwrap();

            temp_file.write_all(&body).unwrap();
            println!("load {} file", temp_file.path().display());
            let mut gh_source_gzip =
                gharchive::GithubArchiveSource::from_file(temp_file.path()).unwrap();
            gh_source_gzip.init().await.unwrap_or_else(|e| {
                assert!(false, "init failed : {:?}", e);
            });

            for n in 0..2000000 {
                let ret = gh_source_gzip.next().await;
                let mut should_exit = false;
                ret.map_or_else(
                    |e| assert!(false, "next in {} failed : {:?}", n, e),
                    |line| {
                        println!("n={} test={:?}", n, line);
                        should_exit = line.is_none();
                    },
                );
                if should_exit {
                    println!("test finish");
                    break;
                }
            }
        } else {
            assert!(
                false,
                "{}",
                format!("download file error, status={:?}", response.status())
            );
        }
    }

    #[cfg(feature = "gharchive")]
    #[test]
    async fn test_http_source() {
        let uri = "https://data.gharchive.org/2015-01-01-15.json.gz";
        let gh_source_gzip = gharchive::GithubArchiveSource::new(uri);
        assert!(gh_source_gzip.is_some());

        let mut gh_source_gzip = gh_source_gzip.unwrap();
        gh_source_gzip.set_io_timeout(std::time::Duration::from_secs(20));

        gh_source_gzip.init().await.unwrap_or_else(|e| {
            assert!(false, "init failed : {:?}", e);
        });

        for n in 0..200000 {
            let ret = gh_source_gzip.next().await;

            let mut should_exit = false;
            ret.map_or_else(
                |e| assert!(false, "next in {} failed : {:?}", n, e),
                |line| {
                    println!("n={} test={:?}", n, line);
                    should_exit = line.is_none();
                },
            );
            if should_exit {
                println!("finished test");
                break;
            }
        }
    }

    #[cfg(feature = "gharchive")]
    #[test]
    async fn test_date_range() {
        let mut source = gharchive::GithubArchiveSource::from_date("2021-01-01").unwrap();

        source.set_end_date("2021-01-01").unwrap();
        source.set_io_timeout(std::time::Duration::from_secs(20));

        source.init().await.unwrap();

        let mut record_count = 0;
        let max_records = 200000;

        loop {
            match source.next().await {
                Ok(Some(_record)) => {
                    record_count += 1;
                    if record_count >= max_records {
                        break;
                    }
                }
                Ok(None) => {
                    println!("Reached end of date range as expected");
                    break;
                }
                Err(e) => {
                    assert!(false, "Error during processing: {:?}", e);
                    break;
                }
            }
        }

        assert!(
            record_count == max_records,
            "Processed {} records from date range, expected {}",
            record_count,
            max_records
        );
    }
}
