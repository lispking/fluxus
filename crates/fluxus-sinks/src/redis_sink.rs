use crate::Sink;
use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamResult};
use redis::{AsyncCommands, Client};
use serde::Serialize;
use serde_json;
use std::marker::PhantomData;

/// Redis operation type
#[derive(Clone, Debug)]
pub enum RedisOperation {
    /// SET key value - overwrites the key
    Set { key_field: String },
    /// HSET hash field value - sets field in hash
    HSet { hash_name: String, field_name: String },
    /// LPUSH key value - pushes to list (left side)
    LPush { list_name: String },
    /// RPUSH key value - pushes to list (right side)
    RPush { list_name: String },
    /// XADD stream * field value - adds to Redis stream
    StreamAdd { stream_name: String },
}

/// A sink that writes to Redis
pub struct RedisSink<T> {
    connection_url: String,
    operation: RedisOperation,
    client: Option<Client>,
    connection: Option<redis::aio::Connection>,
    _phantom: PhantomData<T>,
}

impl<T> RedisSink<T> {
    /// Create a new Redis sink
    pub fn new(connection_url: String, operation: RedisOperation) -> Self {
        Self {
            connection_url,
            operation,
            client: None,
            connection: None,
            _phantom: PhantomData,
        }
    }

    /// Create a Redis sink for SET operations
    pub fn set<S: Into<String>>(connection_url: String, key_field: S) -> Self {
        Self::new(
            connection_url,
            RedisOperation::Set {
                key_field: key_field.into(),
            },
        )
    }

    /// Create a Redis sink for HSET operations
    pub fn hset<S: Into<String>>(connection_url: String, hash_name: S, field_name: S) -> Self {
        Self::new(
            connection_url,
            RedisOperation::HSet {
                hash_name: hash_name.into(),
                field_name: field_name.into(),
            },
        )
    }

    /// Create a Redis sink for LPUSH operations
    pub fn lpush<S: Into<String>>(connection_url: String, list_name: S) -> Self {
        Self::new(
            connection_url,
            RedisOperation::LPush {
                list_name: list_name.into(),
            },
        )
    }

    /// Create a Redis sink for RPUSH operations
    pub fn rpush<S: Into<String>>(connection_url: String, list_name: S) -> Self {
        Self::new(
            connection_url,
            RedisOperation::RPush {
                list_name: list_name.into(),
            },
        )
    }

    /// Create a Redis sink for Stream operations (XADD)
    pub fn stream<S: Into<String>>(connection_url: String, stream_name: S) -> Self {
        Self::new(
            connection_url,
            RedisOperation::StreamAdd {
                stream_name: stream_name.into(),
            },
        )
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync> Sink<T> for RedisSink<T> {
    async fn init(&mut self) -> StreamResult<()> {
        let client = Client::open(self.connection_url.as_str())
            .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;
        
        let connection = client
            .get_async_connection()
            .await
            .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;

        self.client = Some(client);
        self.connection = Some(connection);
        Ok(())
    }

    async fn write(&mut self, record: Record<T>) -> StreamResult<()> {
        if let Some(conn) = &mut self.connection {
            let json_value = serde_json::to_string(&record.data)?;
            
            match &self.operation {
                RedisOperation::Set { key_field } => {
                    let key = format!("{key_field}:{}", record.timestamp);
                    conn.set::<&str, &str, ()>(&key, &json_value)
                        .await
                        .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;
                }
                RedisOperation::HSet { hash_name, field_name } => {
                    let field = format!("{field_name}:{}", record.timestamp);
                    conn.hset::<&str, &str, &str, ()>(hash_name, &field, &json_value)
                        .await
                        .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;
                }
                RedisOperation::LPush { list_name } => {
                    conn.lpush::<&str, &str, ()>(list_name, &json_value)
                        .await
                        .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;
                }
                RedisOperation::RPush { list_name } => {
                    conn.rpush::<&str, &str, ()>(list_name, &json_value)
                        .await
                        .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;
                }
                RedisOperation::StreamAdd { stream_name } => {
                    // Use XADD to add to Redis stream with timestamp and data
                    let timestamp_str = record.timestamp.to_string();
                    let fields = vec![("data", json_value.as_str()), ("timestamp", timestamp_str.as_str())];
                    conn.xadd::<&str, &str, &str, &str, ()>(stream_name, "*", &fields)
                        .await
                        .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;
                }
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<()> {
        // Redis operations are immediately committed, no explicit flush needed
        Ok(())
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.connection = None;
        self.client = None;
        Ok(())
    }
}
