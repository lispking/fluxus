use super::Source;
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamError, StreamResult};
use futures::TryStreamExt;
use futures::future::Either;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use url::Url;

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
    pub id: Option<i64>,
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Actor {
    pub id: Option<i64>,
    pub login: Option<String>,
    pub gravatar_id: Option<String>,
    pub avatar_url: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Org {
    pub id: Option<i64>,
    pub login: Option<String>,
    pub gravatar_id: Option<String>,
    pub avatar_url: String,
    pub url: String,
}

/// A source that reads GitHub archive source files
pub struct GithubArchiveSource {
    uri: Url,
    reader: Option<Box<dyn tokio::io::AsyncBufRead + Unpin + Send + Sync>>,
    io_timeout: Option<Duration>,
}

impl GithubArchiveSource {
    pub fn new<T: TryInto<Url>>(uri: T) -> Option<Self> {
        let uri = uri.try_into().ok()?;
        Some(Self {
            uri,
            reader: None,
            io_timeout: Some(Duration::from_secs(100)),
        })
    }

    pub fn from_file<P: Into<PathBuf>>(path: P) -> Option<Self> {
        let path = path.into();
        let uri_info = if path.is_absolute() {
            Url::from_file_path(path).ok()
        } else {
            Url::from_file_path(std::env::current_dir().ok()?.join(path)).ok()
        };

        uri_info.map(|uri| Self {
            uri,
            reader: None,
            io_timeout: None,
        })
    }

    pub fn set_io_timeout(&mut self, io_timeout: Duration) {
        self.io_timeout = Some(io_timeout);
    }
}

impl GithubArchiveSource {
    async fn init_file(&mut self) -> StreamResult<()> {
        let file = File::open(self.uri.path()).await?;
        if self.uri.path().ends_with(".gz") {
            let buf_reader = BufReader::new(file);
            let decompressed = BufReader::new(GzipDecoder::new(buf_reader));
            self.reader = Some(Box::new(decompressed));
        } else {
            self.reader = Some(Box::new(BufReader::new(file)));
        }

        Ok(())
    }
    async fn init_http(&mut self) -> StreamResult<()> {
        let client = reqwest::Client::builder()
            .timeout(self.io_timeout.unwrap_or(Duration::from_secs(10)))
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
                self.reader = Some(Box::new(BufReader::new(tokio_util::io::StreamReader::new(
                    async_read,
                ))));
            }
            Ok(())
        } else {
            Err(StreamError::Io(Error::new(
                ErrorKind::Other,
                format!(
                    "gharchive request failed, http status is {}",
                    response.status()
                ),
            )))
        }
    }
}

#[async_trait]
impl Source<Event> for GithubArchiveSource {
    async fn init(&mut self) -> StreamResult<()> {
        let scheme = self.uri.scheme();
        if scheme == "http" || scheme == "https" {
            self.init_http().await
        } else if scheme == "file" {
            self.init_file().await
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
        self.reader
            .as_mut()
            .map_or_else(
                || Either::Left(async { Ok(None) }),
                |reader| {
                    Either::Right(async {
                        let mut line = String::new();
                        let read_result = reader.read_line(&mut line).await?;
                        if read_result == 0 {
                            return Ok(None);
                        }
                        let event: Event = serde_json::from_str(&line)?;
                        Ok(Some(Record::new(event)))
                    })
                },
            )
            .await
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.reader = None;
        Ok(())
    }
}
