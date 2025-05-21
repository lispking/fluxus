use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamError, StreamResult};
use futures::TryStreamExt;
use reqwest;
use std::io::{self, Error};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;

use super::Source;

/// A source that reads CSV files
pub struct CsvSource {
    source: CsvSourceType,
    reader: Option<Box<dyn tokio::io::AsyncBufRead + Unpin + Send + Sync>>,
}

enum CsvSourceType {
    LocalFile(PathBuf),
    RemoteUrl(String),
}

impl CsvSource {
    /// Create a new CSV source from a local file path
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            source: CsvSourceType::LocalFile(path.into()),
            reader: None,
        }
    }

    /// Create a new CSV source from a remote URL
    pub fn from_url<S: Into<String>>(url: S) -> Self {
        Self {
            source: CsvSourceType::RemoteUrl(url.into()),
            reader: None,
        }
    }
}

#[async_trait]
impl Source<String> for CsvSource {
    async fn init(&mut self) -> StreamResult<()> {
        match &self.source {
            CsvSourceType::LocalFile(path) => {
                let file = File::open(path)
                    .await
                    .map_err(|e| StreamError::Io(Error::other(format!("{}", e))))?;
                self.reader = Some(Box::new(BufReader::new(file)));
            }
            CsvSourceType::RemoteUrl(url) => {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .map_err(|_e| StreamError::Io(io::Error::other("create http client error")))?;
                let response = client.get(url).send().await.map_err(|e| {
                    StreamError::Io(Error::other(format!("Failed to fetch URL: {}", e)))
                })?;

                if !response.status().is_success() {
                    return Err(StreamError::Io(Error::other(format!(
                        "HTTP error: {}",
                        response.status()
                    ))));
                }

                let byte_stream = response
                    .bytes_stream()
                    .map_err(|e| Error::other(format!("{}", e)));

                let reader = StreamReader::new(byte_stream);
                self.reader = Some(Box::new(BufReader::new(reader)));
            }
        }
        Ok(())
    }

    async fn next(&mut self) -> StreamResult<Option<Record<String>>> {
        if let Some(reader) = &mut self.reader {
            let mut line = String::new();
            match reader.read_line(&mut line).await {
                Ok(0) => Ok(None), // EOF
                Ok(_) => {
                    let line = line.trim().to_string();
                    Ok(Some(Record::new(line)))
                }
                Err(e) => Err(e.into()),
            }
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.reader = None;
        Ok(())
    }
}
