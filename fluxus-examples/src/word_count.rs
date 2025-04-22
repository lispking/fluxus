use fluxus_api::{DataStream, io::{CollectionSource, CollectionSink}};
use fluxus_core::WindowConfig;
use anyhow::Result;
use std::collections::HashMap;

pub type WordCount = HashMap<String, usize>;

pub async fn run() -> Result<()> {
    // Sample input text
    let text = vec![
        "hello world",
        "hello stream processing",
        "world of streaming",
        "hello streaming world",
    ];

    // Create a source from the text collection
    let source = CollectionSource::new(text);
    let sink: CollectionSink<WordCount> = CollectionSink::new();

    // Build and execute the streaming pipeline
    DataStream::new(source)
        // Split text into words
        .map(|line| {
            line.split_whitespace()
                .map(|s| s.to_lowercase())
                .collect::<Vec<_>>()
        })
        // Parallelize the processing
        .parallel(2)
        // Create tumbling windows of 1 second
        .window(WindowConfig::Tumbling { size_ms: 1000 })
        // Count words in each window
        .aggregate(
            HashMap::new(),
            |mut counts, words| {
                for word in words {
                    *counts.entry(word).or_insert(0) += 1;
                }
                counts
            },
        )
        // Write results to sink
        .sink(sink.clone())
        .await?;

    // Print the results
    println!("\nWord count results:");
    for result in sink.get_data() {
        println!("\nWindow results:");
        let mut words: Vec<_> = result.iter().collect();
        words.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        for (word, count) in words {
            println!("  {}: {}", word, count);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_word_count() -> Result<()> {
        // Test input
        let text = vec![
            "test word",
            "another test",
            "word count test",
        ];

        let source = CollectionSource::new(text);
        let sink: CollectionSink<WordCount> = CollectionSink::new();

        DataStream::new(source)
            .map(|line| line.split_whitespace().map(|s| s.to_lowercase()).collect::<Vec<_>>())
            .window(WindowConfig::Tumbling { size_ms: 500 })
            .aggregate(
                HashMap::new(),
                |mut counts, words| {
                    for word in words {
                        *counts.entry(word).or_insert(0) += 1;
                    }
                    counts
                },
            )
            .sink(sink.clone())
            .await?;

        let results = sink.get_data();
        assert!(!results.is_empty());
        
        if let Some(counts) = results.first() {
            assert_eq!(*counts.get("test").unwrap(), 3);
            assert_eq!(*counts.get("word").unwrap(), 2);
        }

        Ok(())
    }
}