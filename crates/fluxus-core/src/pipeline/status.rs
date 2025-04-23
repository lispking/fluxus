/// Status of a pipeline execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    /// Pipeline is initialized but not running
    Ready,
    /// Pipeline is currently running
    Running,
    /// Pipeline has completed successfully
    Completed,
    /// Pipeline has failed
    Failed,
}
