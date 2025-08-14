use crate::Sink;
use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamResult};
use serde::Serialize;
use serde_json::Value;
use std::marker::PhantomData;
use tokio_postgres::{Client, NoTls, types::ToSql};

/// Column mapping configuration for PostgreSQL
#[derive(Debug, Clone)]
pub struct ColumnMapping {
    /// Column name in PostgreSQL table
    pub column_name: String,
    /// JSON path to extract value (e.g., "user.id", "event_type")
    pub json_path: String,
    /// PostgreSQL column type for proper casting
    pub column_type: PostgresType,
    /// Whether this column is required (will error if missing)
    pub required: bool,
}

/// Supported PostgreSQL column types
#[derive(Debug, Clone)]
pub enum PostgresType {
    Text,
    Integer,
    BigInt,
    Double,
    Boolean,
    Timestamp,
    Json,
    Jsonb,
}

impl PostgresType {
    fn to_cast_string(&self) -> &'static str {
        match self {
            PostgresType::Text => "::text",
            PostgresType::Integer => "::integer",
            PostgresType::BigInt => "::bigint",
            PostgresType::Double => "::double precision",
            PostgresType::Boolean => "::boolean",
            PostgresType::Timestamp => "::timestamp",
            PostgresType::Json => "::json",
            PostgresType::Jsonb => "::jsonb",
        }
    }
}

/// Configuration for PostgreSQL sink
pub struct PostgresConfig {
    /// Database connection string
    pub connection_string: String,
    /// Table name to insert into
    pub table_name: String,
    /// Column mappings from JSON to PostgreSQL columns
    pub column_mappings: Vec<ColumnMapping>,
    /// Whether to include record timestamp as a column
    pub include_timestamp: Option<String>,
    /// Batch size for bulk inserts (default: 1)
    pub batch_size: usize,
}

/// A PostgreSQL sink that maps JSON data to specific table columns
pub struct PostgresSink<T> {
    config: PostgresConfig,
    client: Option<Client>,
    batch: Vec<Record<T>>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize> PostgresSink<T> {
    /// Create a new PostgreSQL sink
    pub fn new(config: PostgresConfig) -> Self {
        let batch_capacity = config.batch_size.max(1);
        Self {
            config,
            client: None,
            batch: Vec::with_capacity(batch_capacity),
            _phantom: PhantomData,
        }
    }

    /// Builder pattern for creating PostgreSQL sink
    pub fn builder<S: Into<String>>(connection_string: S, table_name: S) -> PostgresSinkBuilder {
        PostgresSinkBuilder {
            connection_string: connection_string.into(),
            table_name: table_name.into(),
            column_mappings: Vec::new(),
            include_timestamp: None,
            batch_size: 1,
        }
    }

    /// Extract value from JSON using dot notation path
    fn extract_json_value(json_value: &Value, path: &str) -> Option<Value> {
        // Special case: "." means the entire JSON value
        if path == "." {
            return Some(json_value.clone());
        }

        let parts: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
        let mut current = json_value;

        for part in parts {
            match current {
                Value::Object(obj) => {
                    current = obj.get(part)?;
                }
                _ => return None,
            }
        }

        Some(current.clone())
    }

    /// Build INSERT query with proper parameter placeholders
    fn build_insert_query(&self, batch_size: usize) -> String {
        let mut columns = Vec::new();
        let mut value_placeholders = Vec::new();

        // Add mapped columns
        for mapping in &self.config.column_mappings {
            columns.push(mapping.column_name.clone());
        }

        // Add timestamp column if configured
        if let Some(ts_col) = &self.config.include_timestamp {
            columns.push(ts_col.clone());
        }

        // Build value placeholders for batch insert
        for batch_idx in 0..batch_size {
            let mut row_placeholders = Vec::new();
            let base_param_idx = batch_idx * columns.len();

            for (col_idx, mapping) in self.config.column_mappings.iter().enumerate() {
                let param_idx = base_param_idx + col_idx + 1;
                row_placeholders.push(format!(
                    "${param_idx}{}",
                    mapping.column_type.to_cast_string()
                ));
            }

            // Add timestamp parameter if configured
            if self.config.include_timestamp.is_some() {
                let param_idx = base_param_idx + self.config.column_mappings.len() + 1;
                row_placeholders.push(format!("${param_idx}::timestamp"));
            }

            value_placeholders.push(format!("({})", row_placeholders.join(", ")));
        }

        format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.config.table_name,
            columns.join(", "),
            value_placeholders.join(", ")
        )
    }

    /// Execute batch insert
    async fn flush_batch(&mut self) -> StreamResult<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        if let Some(client) = &self.client {
            let query = self.build_insert_query(self.batch.len());
            let mut params: Vec<Box<dyn ToSql + Send + Sync>> = Vec::new();

            for record in &self.batch {
                let json_value = serde_json::to_value(&record.data)?;

                // Add parameters for each column mapping
                for mapping in &self.config.column_mappings {
                    if let Some(value) = Self::extract_json_value(&json_value, &mapping.json_path) {
                        match &mapping.column_type {
                            PostgresType::Text => {
                                let text_val = value.as_str().unwrap_or("").to_string();
                                params.push(Box::new(text_val));
                            }
                            PostgresType::Integer => {
                                let int_val = value.as_i64().unwrap_or(0) as i32;
                                params.push(Box::new(int_val));
                            }
                            PostgresType::BigInt => {
                                let bigint_val = value.as_i64().unwrap_or(0);
                                params.push(Box::new(bigint_val));
                            }
                            PostgresType::Double => {
                                let double_val = value.as_f64().unwrap_or(0.0);
                                params.push(Box::new(double_val));
                            }
                            PostgresType::Boolean => {
                                let bool_val = value.as_bool().unwrap_or(false);
                                params.push(Box::new(bool_val));
                            }
                            PostgresType::Json | PostgresType::Jsonb => {
                                let json_val = serde_json::to_value(&value)?;
                                params.push(Box::new(json_val));
                            }
                            PostgresType::Timestamp => {
                                // Assume timestamp is in milliseconds
                                let ts_ms = value.as_i64().unwrap_or(0);
                                let ts_secs = ts_ms / 1000;
                                let naive_dt = chrono::DateTime::from_timestamp(ts_secs, 0)
                                    .unwrap_or_default()
                                    .naive_utc();
                                params.push(Box::new(naive_dt));
                            }
                        }
                    } else if mapping.required {
                        return Err(fluxus_utils::models::StreamError::Serialization(format!(
                            "Required field '{json_path}' not found in JSON data",
                            json_path = mapping.json_path
                        )));
                    } else {
                        // Use NULL for optional missing fields
                        params.push(Box::new(Option::<String>::None));
                    }
                }

                // Add timestamp parameter if configured
                if self.config.include_timestamp.is_some() {
                    let ts_secs = record.timestamp / 1000;
                    let naive_dt = chrono::DateTime::from_timestamp(ts_secs, 0)
                        .unwrap_or_default()
                        .naive_utc();
                    params.push(Box::new(naive_dt));
                }
            }

            // Convert to references for the query
            let param_refs: Vec<&(dyn ToSql + Sync)> = params
                .iter()
                .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                .collect();

            client
                .execute(&query, &param_refs)
                .await
                .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;
        }

        self.batch.clear();
        Ok(())
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync> Sink<T> for PostgresSink<T> {
    async fn init(&mut self) -> StreamResult<()> {
        let (client, connection) = tokio_postgres::connect(&self.config.connection_string, NoTls)
            .await
            .map_err(|e| fluxus_utils::models::StreamError::Runtime(e.to_string()))?;

        // Spawn the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        self.client = Some(client);
        Ok(())
    }

    async fn write(&mut self, record: Record<T>) -> StreamResult<()> {
        self.batch.push(record);

        if self.batch.len() >= self.config.batch_size {
            self.flush_batch().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<()> {
        self.flush_batch().await
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.flush_batch().await?;
        self.client = None;
        Ok(())
    }
}

/// Builder for PostgresSink
pub struct PostgresSinkBuilder {
    connection_string: String,
    table_name: String,
    column_mappings: Vec<ColumnMapping>,
    include_timestamp: Option<String>,
    batch_size: usize,
}

impl PostgresSinkBuilder {
    /// Add a column mapping
    pub fn map_column<S1: Into<String>, S2: Into<String>>(
        mut self,
        column_name: S1,
        json_path: S2,
        column_type: PostgresType,
        required: bool,
    ) -> Self {
        self.column_mappings.push(ColumnMapping {
            column_name: column_name.into(),
            json_path: json_path.into(),
            column_type,
            required,
        });
        self
    }

    /// Include record timestamp as a column
    pub fn with_timestamp<S: Into<String>>(mut self, column_name: S) -> Self {
        self.include_timestamp = Some(column_name.into());
        self
    }

    /// Set batch size for bulk inserts
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Build the PostgreSQL sink
    pub fn build<T: Serialize>(self) -> PostgresSink<T> {
        PostgresSink::new(PostgresConfig {
            connection_string: self.connection_string,
            table_name: self.table_name,
            column_mappings: self.column_mappings,
            include_timestamp: self.include_timestamp,
            batch_size: self.batch_size,
        })
    }
}

/// Convenience methods for common use cases (backward compatibility)
impl<T: Serialize> PostgresSink<T> {
    /// Create a simple JSONB sink
    pub fn simple_jsonb<S: Into<String>>(
        connection_string: S,
        table_name: S,
        json_column: S,
    ) -> Self {
        Self::builder(connection_string, table_name)
            .map_column(json_column, ".".to_string(), PostgresType::Jsonb, true)
            .build()
    }

    /// Create a JSONB sink with timestamp
    pub fn jsonb_with_timestamp<S: Into<String>>(
        connection_string: S,
        table_name: S,
        json_column: S,
        timestamp_column: S,
    ) -> Self {
        Self::builder(connection_string, table_name)
            .map_column(json_column, ".".to_string(), PostgresType::Jsonb, true)
            .with_timestamp(timestamp_column)
            .build()
    }
}
