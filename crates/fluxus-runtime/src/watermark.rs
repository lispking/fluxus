use parking_lot::RwLock;
use std::sync::Arc;
use std::time::SystemTime;

/// Watermark tracker for managing event time progress
pub struct WatermarkTracker {
    current_watermark: Arc<RwLock<SystemTime>>,
}

impl Default for WatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self {
            current_watermark: Arc::new(RwLock::new(SystemTime::now())),
        }
    }

    pub fn update(&self, watermark: SystemTime) {
        let mut current = self.current_watermark.write();
        if watermark > *current {
            *current = watermark;
        }
    }

    pub fn get_current(&self) -> SystemTime {
        *self.current_watermark.read()
    }
}
