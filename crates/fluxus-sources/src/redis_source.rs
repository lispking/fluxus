use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamError, StreamResult};
use redis::{AsyncCommands, Client, Value};
use serde::de::DeserializeOwned;
use serde_json;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time;

/// Redis queue operation type for consuming data
#[derive(Clone, Debug)]
pub enum RedisQueueOperation {
    /// RPOP from list (blocking with timeout)
    RPop { list_name: String, timeout_secs: u64 },
    /// LPOP from list (blocking with timeout)
    LPop { list_name: String, timeout_secs: u64 },
    /// XREAD from stream (blocking with timeout)
    StreamRead { stream_name: String, timeout_secs: u64, last_id: String },
}

/// A source that reads from Redis queues
pub struct RedisSource<T> {
    connection_url: String,
    operation: RedisQueueOperation,
    client: Option<Client>,
    connection: Option<redis::aio::Connection>,
    _phantom: PhantomData<T>,
}

impl<T> RedisSource<T> {
    /// Create a new Redis source
    pub fn new(connection_url: String, operation: RedisQueueOperation) -> Self {
        Self {
            connection_url,
            operation,
            client: None,
            connection: None,
            _phantom: PhantomData,
        }
    }

    /// Create a Redis source for RPOP operations (consumes from right side of list)
    pub fn rpop<S: Into<String>>(connection_url: String, list_name: S, timeout_secs: u64) -> Self {
        Self::new(
            connection_url,
            RedisQueueOperation::RPop {
                list_name: list_name.into(),
                timeout_secs,
            },
        )
    }

    /// Create a Redis source for LPOP operations (consumes from left side of list)
    pub fn lpop<S: Into<String>>(connection_url: String, list_name: S, timeout_secs: u64) -> Self {
        Self::new(
            connection_url,
            RedisQueueOperation::LPop {
                list_name: list_name.into(),
                timeout_secs,
            },
        )
    }

    /// Create a Redis source for Stream operations (XREAD)
    pub fn stream<S: Into<String>>(
        connection_url: String,
        stream_name: S,
        timeout_secs: u64,
        last_id: Option<S>,
    ) -> Self {
        Self::new(
            connection_url,
            RedisQueueOperation::StreamRead {
                stream_name: stream_name.into(),
                timeout_secs,
                last_id: last_id.map(|id| id.into()).unwrap_or_else(|| "$".to_string()),
            },
        )
    }

    async fn init(&mut self) -> StreamResult<()> {
        let client = Client::open(self.connection_url.as_str())
            .map_err(|e| StreamError::Runtime(e.to_string()))?;
        
        let connection = client
            .get_async_connection()
            .await
            .map_err(|e| StreamError::Runtime(e.to_string()))?;

        self.client = Some(client);
        self.connection = Some(connection);
        Ok(())
    }

    async fn read_next(&mut self) -> StreamResult<Option<Record<T>>>
    where
        T: DeserializeOwned,
    {
        self.ensure_connected().await?;
        
        match &mut self.operation {
            RedisQueueOperation::RPop { list_name, timeout_secs } => {
                let list_name = list_name.clone();
                let timeout = *timeout_secs;
                Self::read_from_list(self.connection.as_mut().unwrap(), &list_name, timeout, true).await
            }
            RedisQueueOperation::LPop { list_name, timeout_secs } => {
                let list_name = list_name.clone();
                let timeout = *timeout_secs;
                Self::read_from_list(self.connection.as_mut().unwrap(), &list_name, timeout, false).await
            }
            RedisQueueOperation::StreamRead { stream_name, timeout_secs, last_id } => {
                let stream_name = stream_name.clone();
                let timeout = *timeout_secs;
                Self::read_from_stream(self.connection.as_mut().unwrap(), &stream_name, timeout, last_id).await
            }
        }
    }

    async fn ensure_connected(&mut self) -> StreamResult<()> {
        if self.connection.is_none() {
            self.init().await?;
        }
        Ok(())
    }

    async fn read_from_list(
        conn: &mut redis::aio::Connection,
        list_name: &str,
        timeout_secs: u64,
        is_right_pop: bool,
    ) -> StreamResult<Option<Record<T>>>
    where
        T: DeserializeOwned,
    {
        let result: Option<(String, String)> = if is_right_pop {
            conn.brpop(list_name, timeout_secs as f64).await
        } else {
            conn.blpop(list_name, timeout_secs as f64).await
        }
        .map_err(|e| StreamError::Runtime(e.to_string()))?;

        match result {
            Some((_, json_data)) => {
                let data: T = serde_json::from_str(&json_data)?;
                Ok(Some(Record::new(data)))
            }
            None => Ok(None), // Timeout
        }
    }

    async fn read_from_stream(
        conn: &mut redis::aio::Connection,
        stream_name: &str,
        timeout_secs: u64,
        last_id: &mut String,
    ) -> StreamResult<Option<Record<T>>>
    where
        T: DeserializeOwned,
    {
        let result: Value = conn
            .xread_options(
                &[stream_name],
                &[last_id.as_str()],
                &redis::streams::StreamReadOptions::default()
                    .block((timeout_secs * 1000) as usize)
                    .count(1),
            )
            .await
            .map_err(|e| StreamError::Runtime(e.to_string()))?;

        Self::parse_stream_response(result, last_id)
    }

    fn parse_stream_response(
        result: Value,
        last_id: &mut String,
    ) -> StreamResult<Option<Record<T>>>
    where
        T: DeserializeOwned,
    {
        let streams = match result {
            Value::Bulk(streams) => streams,
            _ => return Ok(None),
        };

        let stream_data = match streams.first() {
            Some(Value::Bulk(data)) if data.len() >= 2 => data,
            _ => return Ok(None),
        };

        let messages = match &stream_data[1] {
            Value::Bulk(messages) => messages,
            _ => return Ok(None),
        };

        let message = match messages.first() {
            Some(Value::Bulk(message)) if message.len() >= 2 => message,
            _ => return Ok(None),
        };

        // Update last_id for next read
        if let Value::Data(id_bytes) = &message[0] {
            *last_id = String::from_utf8_lossy(id_bytes).to_string();
        }

        // Parse message fields
        let fields = match &message[1] {
            Value::Bulk(fields) => fields,
            _ => return Ok(None),
        };

        for chunk in fields.chunks(2) {
            if let [Value::Data(field), Value::Data(value)] = chunk {
                if field == b"data" {
                    let json_data = String::from_utf8_lossy(value);
                    let data: T = serde_json::from_str(&json_data)?;
                    return Ok(Some(Record::new(data)));
                }
            }
        }

        Ok(None)
    }
}

use crate::Source;

#[async_trait]
impl<T: DeserializeOwned + Send + Sync> Source<T> for RedisSource<T> {
    async fn init(&mut self) -> StreamResult<()> {
        self.init().await
    }

    async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
        loop {
            match self.read_next().await {
                Ok(Some(record)) => return Ok(Some(record)),
                Ok(None) => {
                    // No data available, wait a bit before retrying
                    time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.connection = None;
        self.client = None;
        Ok(())
    }
}
