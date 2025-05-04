use super::Source;
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
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
use tokio_util::io::StreamReader;
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
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
    cur_date: NaiveDate,
    cur_hour: u32,
}

impl GithubArchiveSource {
    /// Create a new GithubArchiveSource with a specific GitHub Archive URL
    pub fn new<T: TryInto<Url>>(uri: T) -> Option<Self> {
        let uri = uri.try_into().ok()?;
        Some(Self {
            uri,
            reader: None,
            io_timeout: Some(Duration::from_secs(10)),
            start_date: Utc::now().date_naive(),
            end_date: None,
            cur_date: Utc::now().date_naive(),
            cur_hour: 0,
        })
    }

    /// Create a new GithubArchiveSource with a given start date
    /// The source will start processing from 00:00 of the specified date
    ///
    /// Date format should be YYYY-MM-DD
    pub fn from_date(start_date: &str) -> StreamResult<Self> {
        let start_date = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
            .map_err(|e| StreamError::Config(format!("Invalid date format: {}", e)))?;

        let fetch_url = format!("https://data.gharchive.org/{}-{}.json.gz", start_date, 0);
        let uri = Url::parse(&fetch_url)
            .map_err(|e| StreamError::Config(format!("Failed to construct URL: {}", e)))?;

        Ok(Self {
            uri,
            reader: None,
            io_timeout: Some(Duration::from_secs(10)),
            start_date,
            end_date: None,
            cur_date: start_date,
            cur_hour: 0,
        })
    }

    /// Create a new GithubArchiveSource with a specific date and hour
    /// Use this when you need precise control over the starting hour
    ///
    /// Date format should be YYYY-MM-DD
    /// Hour must be between 0 and 23
    pub fn from_hour(start_date: &str, hour: u32) -> StreamResult<Self> {
        let start_date = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
            .map_err(|e| StreamError::Config(format!("Invalid date format: {}", e)))?;

        if hour > 23 {
            return Err(StreamError::Config(
                "Hour must be between 0 and 23".to_string(),
            ));
        }

        let fetch_url = format!("https://data.gharchive.org/{}-{}.json.gz", start_date, hour);
        let uri = Url::parse(&fetch_url)
            .map_err(|e| StreamError::Config(format!("Failed to construct URL: {}", e)))?;

        Ok(Self {
            uri,
            reader: None,
            io_timeout: Some(Duration::from_secs(10)),
            start_date,
            end_date: None,
            cur_date: start_date,
            cur_hour: hour,
        })
    }

    pub fn from_file<P: Into<PathBuf>>(path: P) -> Option<Self> {
        let path = path.into();
        let uri_info = if path.is_absolute() {
            Url::from_file_path(path).ok()
        } else {
            Url::from_file_path(std::env::current_dir().ok()?.join(path)).ok()
        };

        let start_date = Utc::now().date_naive();

        uri_info.map(|uri| Self {
            uri,
            reader: None,
            io_timeout: None,
            start_date,
            end_date: None,
            cur_date: start_date,
            cur_hour: 0,
        })
    }

    pub fn set_io_timeout(&mut self, io_timeout: Duration) {
        self.io_timeout = Some(io_timeout);
    }

    /// Set the start date for data analysis
    ///
    /// Date format should be YYYY-MM-DD
    pub fn set_start_date(&mut self, start_date: &str) -> StreamResult<()> {
        self.start_date = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
            .map_err(|e| StreamError::Config(format!("Invalid start date format: {}", e)))?;

        self.cur_date = self.start_date;
        self.cur_hour = 0;
        Ok(())
    }

    /// Set the end date for data analysis (optional)
    ///
    /// Date format should be YYYY-MM-DD
    pub fn set_end_date(&mut self, end_date: &str) -> StreamResult<()> {
        let end_date = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
            .map_err(|e| StreamError::Config(format!("Invalid end date format: {}", e)))?;

        if end_date < self.start_date {
            return Err(StreamError::Config(
                "End date cannot be earlier than start date".to_string(),
            ));
        }

        self.end_date = Some(end_date);
        Ok(())
    }

    /// Create a URL for a specific date and hour
    fn build_date_url(&self, date: NaiveDate, hour: u32) -> Url {
        let date_str = date.format("%Y-%m-%d").to_string();
        let url_str = format!("https://data.gharchive.org/{}-{}.json.gz", date_str, hour);
        Url::parse(&url_str).expect("Failed to construct URL")
    }

    /// Check if we have reached the end date
    fn is_past_end_date(&self) -> bool {
        match self.end_date {
            Some(end_date) => {
                self.cur_date > end_date || (self.cur_date == end_date && self.cur_hour >= 23)
            }
            None => false,
        }
    }
}

impl GithubArchiveSource {
    async fn init_file(&mut self) -> StreamResult<()> {
        let file = File::open(self.uri.path()).await?;
        self.reader = Some(if self.uri.path().ends_with(".gz") {
            let buf_reader = BufReader::new(file);
            let decompressed = BufReader::new(GzipDecoder::new(buf_reader));
            Box::new(decompressed)
        } else {
            Box::new(BufReader::new(file))
        });

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

        if !response.status().is_success() {
            return Err(StreamError::Io(Error::new(
                ErrorKind::Other,
                format!(
                    "gharchive request failed, http status is {}",
                    response.status()
                ),
            )));
        }

        let is_gzip = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map_or(false, |content_type| content_type == "application/gzip");

        let async_read = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e));

        let stream_reader = StreamReader::new(async_read);
        self.reader = Some(Box::new(BufReader::new(if is_gzip {
            Box::new(GzipDecoder::new(BufReader::new(stream_reader)))
                as Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync>
        } else {
            Box::new(stream_reader) as Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync>
        })));
        Ok(())
    }

    /// Advance to the next hour or day
    async fn advance_next(&mut self) -> StreamResult<bool> {
        self.reader = None;

        self.cur_hour = match self.cur_hour {
            hour if hour < 23 => hour + 1,
            _ => {
                self.cur_date = self.cur_date.succ_opt().unwrap();

                if self.is_past_end_date() {
                    return Ok(false);
                }

                0
            }
        };

        self.uri = self.build_date_url(self.cur_date, self.cur_hour);

        match self.init().await {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::warn!("Failed to initialize next dataset: {}", e);
                Box::pin(self.advance_next()).await
            }
        }
    }
}

#[async_trait]
impl Source<Event> for GithubArchiveSource {
    async fn init(&mut self) -> StreamResult<()> {
        match self.uri.scheme() {
            "http" | "https" => self.init_http().await,
            "file" => self.init_file().await,
            _ => Err(StreamError::Io(Error::new(
                ErrorKind::Other,
                "not support scheme",
            ))),
        }
    }

    /*
    archive file downloaded from https://data.gharchive.org/ must be split by CRLF
    */
    async fn next(&mut self) -> StreamResult<Option<Record<Event>>> {
        let result = self
            .reader
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
            .await;

        match result {
            Ok(Some(record)) => Ok(Some(record)),
            Ok(None) => {
                let advanced = self.advance_next().await?;
                if advanced {
                    self.next().await
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.reader = None;
        Ok(())
    }
}
