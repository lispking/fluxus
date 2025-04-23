pub mod csv;
pub mod generator;

use crate::models::{Record, StreamResult};
pub use csv::CsvSource;
pub use generator::GeneratorSource;

use async_trait::async_trait;

/// Source trait defines the interface for data sources
#[async_trait]
pub trait Source<T> {
    /// Initialize the source
    async fn init(&mut self) -> StreamResult<()>;

    /// Read the next record from the source
    async fn next(&mut self) -> StreamResult<Option<Record<T>>>;

    /// Close the source and release resources
    async fn close(&mut self) -> StreamResult<()>;
}
