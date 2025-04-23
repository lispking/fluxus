use std::time::Duration;

/// Error recovery strategy
#[derive(Debug, Clone)]
pub enum RetryStrategy {
    /// No retry, fail immediately
    NoRetry,
    /// Retry with fixed delay
    Fixed {
        delay: Duration,
        max_attempts: usize,
    },
    /// Retry with exponential backoff
    ExponentialBackoff {
        initial_delay: Duration,
        max_delay: Duration,
        max_attempts: usize,
        multiplier: f64,
    },
}

impl RetryStrategy {
    /// Create a fixed delay retry strategy
    pub fn fixed(delay: Duration, max_attempts: usize) -> Self {
        Self::Fixed {
            delay,
            max_attempts,
        }
    }

    /// Create an exponential backoff retry strategy
    pub fn exponential(
        initial_delay: Duration,
        max_delay: Duration,
        max_attempts: usize,
        multiplier: f64,
    ) -> Self {
        Self::ExponentialBackoff {
            initial_delay,
            max_delay,
            max_attempts,
            multiplier,
        }
    }

    /// Calculate delay for a given attempt
    pub fn get_delay(&self, attempt: usize) -> Option<Duration> {
        match self {
            Self::NoRetry => None,
            Self::Fixed {
                delay,
                max_attempts,
            } => {
                if attempt < *max_attempts {
                    Some(*delay)
                } else {
                    None
                }
            }
            Self::ExponentialBackoff {
                initial_delay,
                max_delay,
                max_attempts,
                multiplier,
            } => {
                if attempt < *max_attempts {
                    let delay = Duration::from_secs_f64(
                        initial_delay.as_secs_f64() * multiplier.powi(attempt as i32),
                    );
                    Some(delay.min(*max_delay))
                } else {
                    None
                }
            }
        }
    }
}
