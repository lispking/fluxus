pub mod operator;
mod transform_base;
mod transform_source;
mod transform_source_with_operator;

pub use operator::{Operator, OperatorBuilder};
pub use transform_base::TransformBase;
pub use transform_source::TransformSource;
pub use transform_source_with_operator::TransformSourceWithOperator;

use fluxus_sources::Source;

pub type InnerSource<T> = dyn Source<T> + Send + Sync;
pub type InnerOperator<T, R> = dyn Operator<T, R> + Send + Sync;
