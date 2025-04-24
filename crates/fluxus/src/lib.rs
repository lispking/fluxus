//! Fluxus - A powerful stream processing framework in Rust
//!
//! Fluxus is a high-performance stream processing framework inspired by Flink,
//! designed to provide a seamless experience for building and running data processing pipelines in Rust.
//! It offers a rich set of APIs and components to handle various data sources, sinks, and transformations.
//!
//! ## Add Dependencies
//! To use Fluxus, you need to add the following dependencies to your `Cargo.toml`:
//! ```toml
//! [dependencies]
//! fluxus = { version = "1", features = ["full"] }
//! ```
//!
//! ## Word Count Example
//! Here is a word count example using Fluxus:
//! ```rust
//! use anyhow::Result;
//! use fluxus::api::{
//!     DataStream,
//!     io::{CollectionSink, CollectionSource},
//! };
//! use fluxus::utils::window::WindowConfig;
//! use std::collections::HashMap;
//! use std::time::Duration;
//!
//! pub type WordCount = HashMap<String, usize>;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Sample input text
//!     let text = vec![
//!         "hello world",
//!         "hello stream processing",
//!         "world of streaming",
//!         "hello streaming world",
//!     ];
//!
//!     // Create a source from the text collection
//!     let source = CollectionSource::new(text);
//!     let sink: CollectionSink<WordCount> = CollectionSink::new();
//!
//!     // Build and execute the streaming pipeline
//!     DataStream::new(source)
//!         // Split text into words
//!         .map(|line| {
//!             line.split_whitespace()
//!                 .map(|s| s.to_lowercase())
//!                 .collect::<Vec<_>>()
//!         })
//!         // Parallelize the processing
//!         .parallel(2)
//!         // Create tumbling windows of 1 second
//!         .window(WindowConfig::tumbling(Duration::from_millis(1000)))
//!         // Count words in each window
//!         .aggregate(HashMap::new(), |mut counts, words| {
//!             for word in words {
//!                 *counts.entry(word).or_insert(0) += 1;
//!             }
//!             counts
//!         })
//!         // Write results to sink
//!         .sink(sink.clone())
//!         .await?;
//!
//!     // Print the results
//!     println!("\nWord count results:");
//!     for result in sink.get_data() {
//!         println!("\nWindow results:");
//!         let mut words: Vec<_> = result.iter().collect();
//!         words.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
//!         for (word, count) in words {
//!             println!("  {}: {}", word, count);
//!         }
//!     }
//!
//!     Ok(())
//! }
//!

#[cfg(feature = "fluxus-api")]
pub mod api {
    pub use fluxus_api::*;
}

#[cfg(feature = "fluxus-core")]
pub mod core {
    pub use fluxus_core::*;
}

#[cfg(feature = "fluxus-runtime")]
pub mod runtime {
    pub use fluxus_runtime::*;
}

#[cfg(feature = "fluxus-sinks")]
pub mod sinks {
    pub use fluxus_sinks::*;
}

#[cfg(feature = "fluxus-sources")]
pub mod sources {
    pub use fluxus_sources::*;
}

#[cfg(feature = "fluxus-transformers")]
pub mod transformers {
    pub use fluxus_transformers::*;
}

#[cfg(feature = "fluxus-utils")]
pub mod utils {
    pub use fluxus_utils::*;
}
