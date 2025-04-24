mod backpressure;
mod retry_strategy;

pub use backpressure::{BackpressureController, BackpressureStrategy};
use fluxus_utils::models::StreamResult;
pub use retry_strategy::RetryStrategy;
use tokio::time::sleep;

/// Error handler for retrying operations
pub struct ErrorHandler {
    strategy: RetryStrategy,
}

impl ErrorHandler {
    /// Create a new error handler with the given retry strategy
    pub fn new(strategy: RetryStrategy) -> Self {
        Self { strategy }
    }

    /// Retry an operation with the configured strategy
    pub async fn retry<F, T>(&self, mut operation: F) -> StreamResult<T>
    where
        F: FnMut() -> StreamResult<T>,
    {
        let mut attempt = 0;
        loop {
            match operation() {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if let Some(delay) = self.strategy.get_delay(attempt) {
                        tracing::warn!(
                            "Operation failed (attempt {}/{}): {}. Retrying after {:?}",
                            attempt + 1,
                            match &self.strategy {
                                RetryStrategy::NoRetry => 1,
                                RetryStrategy::Fixed { max_attempts, .. } => *max_attempts,
                                RetryStrategy::ExponentialBackoff { max_attempts, .. } =>
                                    *max_attempts,
                            },
                            error,
                            delay
                        );
                        sleep(delay).await;
                        attempt += 1;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
    }
}
