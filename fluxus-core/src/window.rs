use std::time::Duration;

/// Window type for stream processing
#[derive(Debug, Clone)]
pub enum WindowType {
    /// Tumbling window with fixed size
    Tumbling(Duration),
    /// Sliding window with size and slide interval
    Sliding(Duration, Duration),
    /// Session window with gap timeout
    Session(Duration),
}

/// Configuration for windowed operations
#[derive(Debug, Clone)]
pub struct WindowConfig {
    /// Type of the window
    pub window_type: WindowType,
    /// Whether to allow late arrivals
    pub allow_lateness: Duration,
    /// Watermark strategy (time to wait before processing)
    pub watermark_delay: Duration,
}

impl WindowConfig {
    /// Create a new tumbling window configuration
    pub fn tumbling(size: Duration) -> Self {
        Self {
            window_type: WindowType::Tumbling(size),
            allow_lateness: Duration::from_secs(0),
            watermark_delay: Duration::from_secs(0),
        }
    }

    /// Create a new sliding window configuration
    pub fn sliding(size: Duration, slide: Duration) -> Self {
        Self {
            window_type: WindowType::Sliding(size, slide),
            allow_lateness: Duration::from_secs(0),
            watermark_delay: Duration::from_secs(0),
        }
    }

    /// Create a new session window configuration
    pub fn session(gap: Duration) -> Self {
        Self {
            window_type: WindowType::Session(gap),
            allow_lateness: Duration::from_secs(0),
            watermark_delay: Duration::from_secs(0),
        }
    }

    /// Set the allowed lateness for this window
    pub fn with_lateness(mut self, lateness: Duration) -> Self {
        self.allow_lateness = lateness;
        self
    }

    /// Set the watermark delay for this window
    pub fn with_watermark_delay(mut self, delay: Duration) -> Self {
        self.watermark_delay = delay;
        self
    }
}
