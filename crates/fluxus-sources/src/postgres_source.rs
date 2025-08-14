use async_trait::async_trait;
use fluxus_utils::models::{Record, StreamError, StreamResult};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time;
use tokio_postgres::{Client, NoTls, Row};

/// PostgreSQL source operation type
#[derive(Clone, Debug)]
pub enum PostgresSourceOperation {
    /// Execute a single query and return all results
    Query { sql: String, params: Vec<String> },
    /// Poll database periodically with a query
    Polling { 
        sql: String, 
        params: Vec<String>, 
        interval: Duration,
        last_value: Option<String>,
    },
}

/// A source that reads from PostgreSQL
pub struct PostgresSource<T> {
    connection_string: String,
    operation: PostgresSourceOperation,
    client: Option<Client>,
    rows: Vec<Row>,
    current_index: usize,
    _phantom: PhantomData<T>,
}

impl<T> PostgresSource<T> {
    /// Create a new PostgreSQL source
    pub fn new(connection_string: String, operation: PostgresSourceOperation) -> Self {
        Self {
            connection_string,
            operation,
            client: None,
            rows: Vec::new(),
            current_index: 0,
            _phantom: PhantomData,
        }
    }

    /// Create a PostgreSQL source for a single query
    pub fn query<S: Into<String>>(connection_string: S, sql: S) -> Self {
        Self::new(
            connection_string.into(),
            PostgresSourceOperation::Query {
                sql: sql.into(),
                params: Vec::new(),
            },
        )
    }

    /// Create a PostgreSQL source for a query with parameters
    pub fn query_with_params<S: Into<String>>(
        connection_string: S,
        sql: S,
        params: Vec<S>,
    ) -> Self {
        Self::new(
            connection_string.into(),
            PostgresSourceOperation::Query {
                sql: sql.into(),
                params: params.into_iter().map(|p| p.into()).collect(),
            },
        )
    }

    /// Create a PostgreSQL source that polls periodically
    pub fn polling<S: Into<String>>(
        connection_string: S,
        sql: S,
        interval: Duration,
    ) -> Self {
        Self::new(
            connection_string.into(),
            PostgresSourceOperation::Polling {
                sql: sql.into(),
                params: Vec::new(),
                interval,
                last_value: None,
            },
        )
    }

    /// Create a PostgreSQL source that polls with parameters
    pub fn polling_with_params<S: Into<String>>(
        connection_string: S,
        sql: S,
        params: Vec<S>,
        interval: Duration,
    ) -> Self {
        Self::new(
            connection_string.into(),
            PostgresSourceOperation::Polling {
                sql: sql.into(),
                params: params.into_iter().map(|p| p.into()).collect(),
                interval,
                last_value: None,
            },
        )
    }

    async fn ensure_connected(&mut self) -> StreamResult<()> {
        if self.client.is_none() {
            let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
                .await
                .map_err(|e| StreamError::Runtime(e.to_string()))?;

            // Spawn the connection task
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("PostgreSQL connection error: {}", e);
                }
            });

            self.client = Some(client);
        }
        Ok(())
    }

    async fn execute_query(&mut self) -> StreamResult<()> {
        self.ensure_connected().await?;
        
        if let Some(client) = &self.client {
            match &self.operation {
                PostgresSourceOperation::Query { sql, params } => {
                    // Convert string params to &str for the query
                    let param_refs: Vec<&str> = params.iter().map(|s| s.as_str()).collect();
                    let param_values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                        param_refs.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                    
                    self.rows = client
                        .query(sql, &param_values)
                        .await
                        .map_err(|e| StreamError::Runtime(e.to_string()))?;
                    
                    self.current_index = 0;
                }
                PostgresSourceOperation::Polling { sql, params, last_value, .. } => {
                    // For polling, we might use last_value as a parameter
                    let mut all_params = params.clone();
                    if let Some(last_val) = last_value {
                        all_params.push(last_val.clone());
                    }
                    
                    let param_refs: Vec<&str> = all_params.iter().map(|s| s.as_str()).collect();
                    let param_values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
                        param_refs.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                    
                    let new_rows = client
                        .query(sql, &param_values)
                        .await
                        .map_err(|e| StreamError::Runtime(e.to_string()))?;
                    
                    self.rows.extend(new_rows);
                }
            }
        }
        
        Ok(())
    }

    fn parse_row_to_record(&self, row: &Row) -> StreamResult<Record<T>>
    where
        T: DeserializeOwned,
    {
        // Try to get the first column as JSON
        if !row.is_empty() {
            // If it's a single column, assume it's JSON
            if row.len() == 1 {
                let json_value: serde_json::Value = row.try_get(0)
                    .map_err(|e| StreamError::Serialization(e.to_string()))?;
                let data: T = serde_json::from_value(json_value)?;
                return Ok(Record::new(data));
            }
            
            // If multiple columns, create a JSON object from all columns
            let mut json_obj = serde_json::Map::new();
            for (i, column) in row.columns().iter().enumerate() {
                let column_name = column.name();
                
                // Handle different PostgreSQL types
                let value: serde_json::Value = match column.type_() {
                    &tokio_postgres::types::Type::TEXT | &tokio_postgres::types::Type::VARCHAR => {
                        let text: Option<String> = row.try_get(i)
                            .map_err(|e| StreamError::Serialization(e.to_string()))?;
                        text.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null)
                    }
                    &tokio_postgres::types::Type::INT4 => {
                        let num: Option<i32> = row.try_get(i)
                            .map_err(|e| StreamError::Serialization(e.to_string()))?;
                        num.map(|n| serde_json::Value::Number(n.into())).unwrap_or(serde_json::Value::Null)
                    }
                    &tokio_postgres::types::Type::INT8 => {
                        let num: Option<i64> = row.try_get(i)
                            .map_err(|e| StreamError::Serialization(e.to_string()))?;
                        num.map(|n| serde_json::Value::Number(n.into())).unwrap_or(serde_json::Value::Null)
                    }
                    &tokio_postgres::types::Type::FLOAT8 => {
                        let num: Option<f64> = row.try_get(i)
                            .map_err(|e| StreamError::Serialization(e.to_string()))?;
                        num.map(|n| serde_json::Number::from_f64(n).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null))
                            .unwrap_or(serde_json::Value::Null)
                    }
                    &tokio_postgres::types::Type::BOOL => {
                        let b: Option<bool> = row.try_get(i)
                            .map_err(|e| StreamError::Serialization(e.to_string()))?;
                        b.map(serde_json::Value::Bool).unwrap_or(serde_json::Value::Null)
                    }
                    &tokio_postgres::types::Type::JSONB | &tokio_postgres::types::Type::JSON => {
                        let json: Option<serde_json::Value> = row.try_get(i)
                            .map_err(|e| StreamError::Serialization(e.to_string()))?;
                        json.unwrap_or(serde_json::Value::Null)
                    }
                    _ => {
                        // For other types, try to convert to string
                        let text: Option<String> = row.try_get(i)
                            .map_err(|e| StreamError::Serialization(e.to_string()))?;
                        text.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null)
                    }
                };
                
                json_obj.insert(column_name.to_string(), value);
            }
            
            let data: T = serde_json::from_value(serde_json::Value::Object(json_obj))?;
            Ok(Record::new(data))
        } else {
            Err(StreamError::Runtime("Empty row received".to_string()))
        }
    }
}

use crate::Source;

#[async_trait]
impl<T: DeserializeOwned + Send + Sync> Source<T> for PostgresSource<T> {
    async fn init(&mut self) -> StreamResult<()> {
        self.ensure_connected().await?;
        
        // For single query, execute immediately
        if matches!(self.operation, PostgresSourceOperation::Query { .. }) {
            self.execute_query().await?;
        }
        
        Ok(())
    }

    async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
        // If we have rows to return, return the next one
        if self.current_index < self.rows.len() {
            let row = &self.rows[self.current_index];
            self.current_index += 1;
            return Ok(Some(self.parse_row_to_record(row)?));
        }

        // For polling operations, check if we need to poll again
        if let PostgresSourceOperation::Polling { interval, .. } = &self.operation {
            time::sleep(*interval).await;
            self.execute_query().await?;
            
            // If we got new rows, return the first one
            if !self.rows.is_empty() {
                let row = &self.rows[0];
                self.current_index = 1;
                return Ok(Some(self.parse_row_to_record(row)?));
            }
        }

        // No more data available
        Ok(None)
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.client = None;
        self.rows.clear();
        self.current_index = 0;
        Ok(())
    }
}
