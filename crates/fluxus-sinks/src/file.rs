use crate::Sink;
use async_trait::async_trait;
use csv;
use fluxus_utils::models::{Record, StreamResult};
use serde::Serialize;
use serde_json;
use std::marker::PhantomData;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Output format for file sink
#[derive(Clone, Debug)]
pub enum FileFormat {
    /// Plain text format (one line per record)
    Text,
    /// CSV format
    Csv,
    /// JSON format (one JSON object per line)
    JsonLines,
}

/// A sink that writes to a file
pub struct FileSink<T> {
    path: PathBuf,
    format: FileFormat,
    file: Option<File>,
    _phantom: PhantomData<T>,
}

impl<T> FileSink<T> {
    /// Create a new file sink
    pub fn new<P: Into<PathBuf>>(path: P, format: FileFormat) -> Self {
        Self {
            path: path.into(),
            format,
            file: None,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Serialize + Send> Sink<T> for FileSink<T> {
    async fn init(&mut self) -> StreamResult<()> {
        self.file = Some(File::create(&self.path).await?);
        Ok(())
    }

    async fn write(&mut self, record: Record<T>) -> StreamResult<()> {
        if let Some(file) = &mut self.file {
            match self.format {
                FileFormat::Text => {
                    let line = format!("{}\n", serde_json::to_string(&record.data)?);
                    file.write_all(line.as_bytes()).await?;
                }
                FileFormat::Csv => {
                    let mut wtr = csv::Writer::from_writer(Vec::new());
                    wtr.serialize(&record.data)?;
                    let inner = wtr.into_inner()?;
                    let data = String::from_utf8(inner)?;
                    file.write_all(data.as_bytes()).await?;
                }
                FileFormat::JsonLines => {
                    let line = format!("{}\n", serde_json::to_string(&record.data)?);
                    file.write_all(line.as_bytes()).await?;
                }
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<()> {
        if let Some(file) = &mut self.file {
            file.flush().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> StreamResult<()> {
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
        }
        Ok(())
    }
}
