use super::Source;
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use chrono;
use fluxus_utils::models::{Record, StreamError, StreamResult};
use futures::TryStreamExt;
use std::io::{Error, ErrorKind};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use url::Url;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub public: bool,
    pub payload: Value,
    pub repo: Repo,
    pub actor: Actor,
    pub org: Option<Org>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Repo {
    pub id: i64,
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Actor {
    pub id: i64,
    pub login: String,
    pub gravatar_id: String,
    pub avatar_url: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Org {
    pub id: i64,
    pub login: String,
    pub gravatar_id: String,
    pub avatar_url: String,
    pub url: String,
}

/// A source that reads github archive source files
pub struct GHarchiveSource {
    uri: Url,
    reader: Option<Box<dyn tokio::io::AsyncBufRead + Unpin + Send + Sync>>,
}

impl GHarchiveSource {
    pub fn new(uri: &str) -> Self {
        let uri_info: Url;
        if let Ok(url) = Url::parse(&uri) {
            uri_info = url;
        } else {
            if Path::new(uri).is_absolute() {
                uri_info = Url::from_file_path(uri).unwrap();
            } else {
                let current_dir = std::env::current_dir().unwrap();
                uri_info = Url::from_file_path(current_dir.join(uri)).unwrap();
            }
        }

        Self {
            uri: uri_info,
            reader: None,
        }
    }
}

#[async_trait]
impl Source<Event> for GHarchiveSource {
    async fn init(&mut self) -> StreamResult<()> {
        let scheme = self.uri.scheme();
        if scheme == "http" || scheme == "https" {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .map_err(|_e| {
                    StreamError::Io(Error::new(ErrorKind::Other, "create http client error"))
                })?;

            let request = client.get(self.uri.clone());

            let response = request.send().await.map_err(|e| {
                StreamError::Io(Error::new(
                    ErrorKind::Other,
                    format!("HTTP request failed:: {}", e),
                ))
            })?;

            let mut is_gzip = false;
            if let Some(content_type) = response.headers().get(reqwest::header::CONTENT_TYPE) {
                if "application/gzip" == content_type {
                    is_gzip = true;
                }
            }

            if response.status().is_success() {
                let async_read = response
                    .bytes_stream()
                    .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e));

                if is_gzip {
                    let gzip_decoder = GzipDecoder::new(BufReader::new(
                        tokio_util::io::StreamReader::new(async_read),
                    ));
                    self.reader = Some(Box::new(BufReader::new(gzip_decoder)));
                } else {
                    self.reader = Some(Box::new(BufReader::new(
                        tokio_util::io::StreamReader::new(async_read),
                    )));
                }

                Ok(())
            } else {
                Err(StreamError::Io(Error::new(
                    ErrorKind::Other,
                    format!(
                        "HTTP request failed: {} code={}",
                        self.uri,
                        response.status()
                    ),
                )))
            }
        } else if scheme == "file" {
            let file = File::open(self.uri.path()).await?;
            if self.uri.path().ends_with(".gz") {
                let buf_reader = BufReader::new(file);
                let decompressed = BufReader::new(GzipDecoder::new(buf_reader));
                self.reader = Some(Box::new(decompressed));
            } else {
                self.reader = Some(Box::new(BufReader::new(file)));
            }

            Ok(())
        } else {
            Err(StreamError::Io(Error::new(
                ErrorKind::Other,
                "not support scheme",
            )))
        }
    }

    /*
    archive file downloaded from https://data.gharchive.org/ must be split by CRLF
    */
    async fn next(&mut self) -> StreamResult<Option<Record<Event>>> {
        if let Some(reader) = &mut self.reader {
            let mut line = String::new();
            match reader.read_line(&mut line).await {
                Ok(0) => Ok(None), // EOF
                Ok(_) => {
                    let event: Event = serde_json::from_str(&line).unwrap();
                    Ok(Some(Record::new(event)))
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
