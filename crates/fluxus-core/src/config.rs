/// Configuration for parallel processing
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of parallel tasks
    pub parallelism: usize,
    /// Maximum buffer size per task
    pub buffer_size: usize,
    /// Whether to preserve ordering in parallel processing
    pub preserve_order: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            parallelism: num_cpus::get(),
            buffer_size: 1000,
            preserve_order: true,
        }
    }
}

impl ParallelConfig {
    /// Create a new parallel configuration
    pub fn new(parallelism: usize, buffer_size: usize, preserve_order: bool) -> Self {
        Self {
            parallelism,
            buffer_size,
            preserve_order,
        }
    }

    /// Set the number of parallel tasks
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Set the buffer size per task
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set whether to preserve ordering
    pub fn with_preserve_order(mut self, preserve_order: bool) -> Self {
        self.preserve_order = preserve_order;
        self
    }
}
