use anyhow::Result;
use fluxus::api::{
    DataStream,
    io::{CollectionSink, CollectionSource},
};
use fluxus::utils::window::WindowConfig;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

#[derive(Clone)]
#[allow(dead_code)]
pub struct LogEntry {
    ip: String,
    method: String,
    path: String,
    status: u16,
    bytes: u64,
    timestamp: SystemTime,
}

#[derive(Clone)]
pub struct PathStats {
    path: String,
    total_requests: usize,
    error_count: usize,
    total_bytes: u64,
    avg_response_size: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Generate sample log entries
    let logs = generate_sample_logs();
    let source = CollectionSource::new(logs);
    let sink = CollectionSink::new();

    // Build and execute the streaming pipeline
    DataStream::new(source)
        // Group by path
        .map(|log| (log.path.clone(), log))
        // Create 60-second sliding windows with 10-second slide
        .window(WindowConfig::sliding(
            Duration::from_millis(60000),
            Duration::from_millis(10000),
        ))
        // Aggregate path statistics
        .aggregate(HashMap::new(), |mut stats, (path, log)| {
            let entry = stats.entry(path).or_insert_with(|| PathStats {
                path: String::new(),
                total_requests: 0,
                error_count: 0,
                total_bytes: 0,
                avg_response_size: 0.0,
            });

            entry.path = log.path;
            entry.total_requests += 1;
            if log.status >= 400 {
                entry.error_count += 1;
            }
            entry.total_bytes += log.bytes;
            entry.avg_response_size = entry.total_bytes as f64 / entry.total_requests as f64;

            stats
        })
        .sink(sink.clone())
        .await?;

    // Print results
    println!("\nNetwork log analysis results:");
    for window_stats in sink.get_data() {
        println!("\nWindow results:");
        for (_, stats) in window_stats {
            println!(
                "Path: {}\n  Requests: {}\n  Errors: {}\n  Avg Size: {:.2} bytes\n  Error Rate: {:.1}%",
                stats.path,
                stats.total_requests,
                stats.error_count,
                stats.avg_response_size,
                (stats.error_count as f64 / stats.total_requests as f64) * 100.0
            );
        }
    }

    Ok(())
}

// Helper function to generate sample data
fn generate_sample_logs() -> Vec<LogEntry> {
    let start_time = SystemTime::now();
    let mut logs = Vec::new();
    let paths = ["/api/users", "/api/products", "/api/orders", "/health"];
    let methods = ["GET", "POST", "PUT", "DELETE"];

    for i in 0..200 {
        let timestamp = start_time + Duration::from_secs(i as u64 / 4);
        let path = paths[i % paths.len()];
        let method = methods[i % methods.len()];

        // Generate a mix of successful and error responses
        let status = if i % 10 == 0 {
            500 // Occasional server errors
        } else if i % 7 == 0 {
            404 // Some not found errors
        } else {
            200 // Mostly successful
        };

        // Simulate variable response sizes
        let bytes = if status == 200 {
            1000 + (i % 5) * 500 // Successful responses have larger sizes
        } else {
            100 + (i % 3) * 50 // Error responses are smaller
        } as u64;

        logs.push(LogEntry {
            ip: format!("192.168.1.{}", i % 256),
            method: method.to_string(),
            path: path.to_string(),
            status,
            bytes,
            timestamp,
        });
    }

    logs
}
