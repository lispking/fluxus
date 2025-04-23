use crate::models::{Record, StreamResult};
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

use super::Source;

/// A source that reads CSV files
pub struct CsvSource {
    path: PathBuf,
    reader: Option<BufReader<File>>,
}

impl CsvSource {
    /// Create a new CSV source from a file path
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            reader: None,
        }
    }
}

#[async_trait]
impl Source<String> for CsvSource {
    async fn init(&mut self) -> StreamResult<()> {
        let file = File::open(&self.path).await?;
        self.reader = Some(BufReader::new(file));
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
