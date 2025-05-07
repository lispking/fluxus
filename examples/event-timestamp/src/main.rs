use anyhow::Result;
use fluxus::utils::{models::Record, window::WindowConfig};
use fluxus::{
    api::{
        DataStream,
        io::{CollectionSink, CollectionSource},
    },
    utils::time::current_time,
};
use std::{collections::HashMap, time::Duration};

pub type EventCount = HashMap<(String, i64), usize>;

#[tokio::main]
async fn main() -> Result<()> {
    // Create timestamped event data
    let events = vec![
        Record::with_timestamp("login".to_string(), get_timestamp(0)),
        Record::with_timestamp("click".to_string(), get_timestamp(100)),
        Record::with_timestamp("click".to_string(), get_timestamp(100)),
        Record::with_timestamp("click".to_string(), get_timestamp(100)),
        Record::with_timestamp("login".to_string(), get_timestamp(200)),
        Record::with_timestamp("purchase".to_string(), get_timestamp(300)),
        Record::with_timestamp("click".to_string(), get_timestamp(400)),
        Record::with_timestamp("click".to_string(), get_timestamp(400)),
        Record::with_timestamp("click".to_string(), get_timestamp(600)),
    ];

    // Create data source and sink
    let source = CollectionSource::new(events);
    let sink: CollectionSink<EventCount> = CollectionSink::new();

    // Build and execute stream processing pipeline
    DataStream::new(source)
        // Create tumbling windows of 1 milliseconds
        .window(WindowConfig::tumbling(Duration::from_millis(1)))
        // Count events in each time window
        .aggregate(HashMap::new(), |mut counts, event| {
            *counts.entry((event.data, event.timestamp)).or_insert(0) += 1;
            counts
        })
        // Write results to sink
        .sink(sink.clone())
        .await?;

    // Print results
    println!("\nEvent counts by timestamp:");
    if let Some(last_result) = sink.get_last_element() {
        println!("\nTime window results:");
        let mut events: Vec<_> = last_result.iter().collect();
        events.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        for (event, count) in events {
            println!("  {event:?}: {count}");
        }
    }

    Ok(())
}

// Helper function: Generate timestamp relative to current time
fn get_timestamp(offset_ms: u64) -> i64 {
    let now = current_time() as i64;
    now + offset_ms as i64
}
