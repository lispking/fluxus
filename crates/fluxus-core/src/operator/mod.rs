use crate::models::{Record, StreamResult};
use async_trait::async_trait;

mod builder;
mod filter;
mod map;
mod window_reduce;

pub use builder::OperatorBuilder;
pub use filter::FilterOperator;
pub use map::MapOperator;
pub use window_reduce::WindowReduceOperator;

/// Operator trait defines the interface for stream processing operators
#[async_trait]
pub trait Operator<In, Out>: Send {
    /// Initialize the operator
    async fn init(&mut self) -> StreamResult<()>;

    /// Process a single record and return zero or more output records
    async fn process(&mut self, record: Record<In>) -> StreamResult<Vec<Record<Out>>>;

    /// Called when a window is triggered (if windowing is enabled)
    async fn on_window_trigger(&mut self) -> StreamResult<Vec<Record<Out>>> {
        Ok(Vec::new())
    }

    /// Close the operator and release resources
    async fn close(&mut self) -> StreamResult<()>;
}
