use super::{FilterOperator, MapOperator, WindowReduceOperator};
use crate::window::WindowConfig;

/// Builder for creating stream operators
pub struct OperatorBuilder;

// Add type aliases at the module level
type AveragePair<T> = (T, usize);
type AverageReduceFn<T> =
    Box<dyn Fn(AveragePair<T>, AveragePair<T>) -> AveragePair<T> + Send + Sync>;

impl OperatorBuilder {
    /// Create a new map operator
    pub fn map<In, Out, F>(func: F) -> MapOperator<In, Out, F>
    where
        F: Fn(In) -> Out + Send + Sync,
    {
        MapOperator::new(func)
    }

    /// Create a new filter operator
    pub fn filter<T, F>(predicate: F) -> FilterOperator<T, F>
    where
        F: Fn(&T) -> bool + Send + Sync,
    {
        FilterOperator::new(predicate)
    }

    /// Create a new window reduce operator
    pub fn window_reduce<T, F>(func: F, window: WindowConfig) -> WindowReduceOperator<T, F>
    where
        F: Fn(T, T) -> T + Send + Sync,
        T: Clone,
    {
        WindowReduceOperator::new(func, window)
    }

    /// Helper to create a sum operator with a window
    pub fn sum_window<T>(window: WindowConfig) -> WindowReduceOperator<T, impl Fn(T, T) -> T>
    where
        T: std::ops::Add<Output = T> + Clone + Send,
    {
        Self::window_reduce(|a, b| a + b, window)
    }

    /// Helper to create a count operator with a window
    pub fn count_window(
        window: WindowConfig,
    ) -> WindowReduceOperator<usize, impl Fn(usize, usize) -> usize> {
        Self::window_reduce(|count, _| count + 1, window)
    }

    /// Helper to create an average operator with a window
    pub fn avg_window<T>(
        window: WindowConfig,
    ) -> WindowReduceOperator<AveragePair<T>, AverageReduceFn<T>>
    where
        T: std::ops::Add<Output = T> + Clone + Send + 'static,
    {
        Self::window_reduce(
            Box::new(|(sum1, count1), (sum2, count2)| (sum1 + sum2, count1 + count2)),
            window,
        )
    }
}
