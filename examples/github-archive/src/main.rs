#[cfg(feature = "gharchive")]
use {
    fluxus::api::{DataStream, io::CollectionSink},
    fluxus::sources::{Source, gharchive},
    fluxus::utils::window::WindowConfig,
    std::time::Duration,
};

use std::collections::HashMap;
pub type EventCount = HashMap<(String, i64), usize>;

#[cfg(feature = "gharchive")]
#[tokio::main]
async fn main() {
    let uri = "https://data.gharchive.org/2015-01-01-15.json.gz";
    let mut gh_source_gzip = gharchive::GithubArchiveSource::new(uri).expect("new failed");
    gh_source_gzip.init().await.expect("init failed");

    process_stream(gh_source_gzip).await;
}

#[cfg(feature = "gharchive")]
async fn run_from_date() {
    let mut gh_source_gzip = gharchive::GithubArchiveSource::from_date("2025-05-04")
        .expect("Failed to create source from date");
    
    gh_source_gzip.set_end_date("2025-05-04").expect("Invalid end date");
    
    gh_source_gzip.init().await.expect("init failed");
    
    process_stream(gh_source_gzip).await;
}

#[cfg(feature = "gharchive")]
async fn process_stream(gh_source_gzip: gharchive::GithubArchiveSource) {
    pub type EventTypeCount = HashMap<String, u32>;

    let sink: CollectionSink<EventTypeCount> = CollectionSink::new();
    DataStream::new(gh_source_gzip)
        .parallel(2)
        .window(WindowConfig::tumbling(Duration::from_millis(1000 * 20)))
        .aggregate(HashMap::new(), |mut counts, event| {
            *counts.entry(event.event_type).or_insert(0) += 1;
            counts
        })
        .sink(sink.clone())
        .await
        .expect("handle error");

    if let Some(result) = sink.get_last_element() {
        let mut events: Vec<_> = result.iter().collect();
        println!("\nWindow results:");
        events.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        for (event, count) in events {
            println!("{}: {}", event, count);
        }
    }
}

#[cfg(not(feature = "gharchive"))]
fn main() {
    println!("GitHub archive example disabled. Use --features gharchive to enable.");
    std::process::exit(1);
}
