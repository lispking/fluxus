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
    /// Global window, no window boundaries
    Global,
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

    /// Create a new global window configuration
    pub fn global() -> Self {
        Self {
            window_type: WindowType::Global,
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

impl WindowType {
    fn get_common_windows(&self, timestamp: i64) -> Vec<i64> {
        match self {
            WindowType::Tumbling(duration) => {
                let duration_ms = duration.as_millis() as i64;
                vec![(timestamp / duration_ms) * duration_ms]
            }
            WindowType::Sliding(size, slide) => {
                let slide_ms = slide.as_millis() as i64;
                let size_ms = size.as_millis() as i64;
                let earliest_window = ((timestamp - size_ms) / slide_ms) * slide_ms;
                let latest_window = (timestamp / slide_ms) * slide_ms;

                (earliest_window..=latest_window)
                    .step_by(slide.as_millis() as usize)
                    .filter(|&start| timestamp - start < size_ms)
                    .collect()
            }
            WindowType::Session(gap) => {
                let gap_ms = gap.as_millis() as i64;
                vec![timestamp / gap_ms]
            }
            WindowType::Global => {
                vec![0]
            }
        }
    }

    pub fn get_affected_windows(&self, timestamp: i64) -> Vec<i64> {
        self.get_common_windows(timestamp)
    }

    pub fn get_window_keys(&self, timestamp: i64) -> Vec<u64> {
        self.get_common_windows(timestamp)
            .iter()
            .map(|&ts| ts as u64)
            .collect()
    }
}
