use std::time::Duration;

/// Backpressure strategy for handling overload
#[derive(Debug, Clone)]
pub enum BackpressureStrategy {
    /// Block when buffer is full
    Block,
    /// Drop oldest items when buffer is full
    DropOldest,
    /// Drop newest items when buffer is full
    DropNewest,
    /// Apply backpressure with custom threshold
    Throttle {
        high_watermark: usize,
        low_watermark: usize,
        backoff: Duration,
    },
}

/// Backpressure controller for managing load
pub struct BackpressureController {
    strategy: BackpressureStrategy,
    current_load: usize,
}

impl BackpressureController {
    /// Create a new backpressure controller with the given strategy
    pub fn new(strategy: BackpressureStrategy) -> Self {
        Self {
            strategy,
            current_load: 0,
        }
    }

    /// Check if we should apply backpressure
    pub fn should_apply_backpressure(&self) -> bool {
        match &self.strategy {
            BackpressureStrategy::Block => self.current_load > 0,
            BackpressureStrategy::DropOldest | BackpressureStrategy::DropNewest => false,
            BackpressureStrategy::Throttle { high_watermark, .. } => {
                self.current_load >= *high_watermark
            }
        }
    }

    /// Get the backoff duration if throttling is needed
    pub fn get_backoff(&self) -> Option<Duration> {
        match &self.strategy {
            BackpressureStrategy::Throttle { backoff, .. } => Some(*backoff),
            _ => None,
        }
    }

    /// Update the current load
    pub fn update_load(&mut self, load: usize) {
        self.current_load = load;
    }

    /// Check if we can accept more items based on the strategy
    pub fn can_accept(&self) -> bool {
        !self.should_apply_backpressure()
    }
}
