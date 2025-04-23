use crate::models::{Record, StreamResult};
use async_trait::async_trait;
use std::marker::PhantomData;

use super::Source;

/// A source that generates test data
pub struct GeneratorSource<T, F>
where
    F: FnMut() -> Option<T> + Send,
{
    generator: F,
    _phantom: PhantomData<T>,
}

impl<T, F> GeneratorSource<T, F>
where
    F: FnMut() -> Option<T> + Send,
{
    /// Create a new generator source
    pub fn new(generator: F) -> Self {
        Self {
            generator,
            _phantom: PhantomData,
        }
    }

    /// Create a counting source that generates numbers from start to end
    pub fn counter(start: i64, end: i64) -> GeneratorSource<i64, impl FnMut() -> Option<i64>> {
        let current = start;
        GeneratorSource::new(move || {
            static mut CURRENT: i64 = 0;
            unsafe {
                if CURRENT == 0 {
                    CURRENT = current;
                }
                if CURRENT <= end {
                    let value = CURRENT;
                    CURRENT += 1;
                    Some(value)
                } else {
                    None
                }
            }
        })
    }
}

#[async_trait]
impl<T, F> Source<T> for GeneratorSource<T, F>
where
    T: Send,
    F: FnMut() -> Option<T> + Send + Sync,
{
    async fn init(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> StreamResult<Option<Record<T>>> {
        Ok((self.generator)().map(Record::new))
    }

    async fn close(&mut self) -> StreamResult<()> {
        Ok(())
    }
}
