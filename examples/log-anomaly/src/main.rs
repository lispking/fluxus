use anyhow::Result;
use fluxus::api::{
    DataStream,
    io::{CollectionSink, CollectionSource},
};
use fluxus::utils::window::WindowConfig;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

#[derive(Clone)]
#[allow(dead_code)]
pub struct LogEvent {
    service: String,
    level: String,
    message: String,
    latency_ms: u64,
    timestamp: SystemTime,
}

#[derive(Clone)]
pub struct AnomalyStats {
    service: String,
    error_rate: f64,
    avg_latency: f64,
    error_count: u32,
    high_latency_count: u32,
    total_events: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Generate sample log events
    let events = generate_sample_events();
    let source = CollectionSource::new(events);
    let sink = CollectionSink::new();

    // Build and execute stream processing pipeline
    DataStream::new(source)
        // Group by service name
        .map(|event| (event.service.clone(), event))
        // Create 1-minute sliding window with 10-second slide
        .window(WindowConfig::sliding(
            Duration::from_secs(60), // 1 minute
            Duration::from_secs(10), // 10 seconds
        ))
        // Aggregate anomaly statistics
        .aggregate(HashMap::new(), |mut stats, (service, event)| {
            let entry = stats
                .entry(service.clone())
                .or_insert_with(|| AnomalyStats {
                    service,
                    error_rate: 0.0,
                    avg_latency: 0.0,
                    error_count: 0,
                    high_latency_count: 0,
                    total_events: 0,
                });

            // Update statistics
            entry.total_events += 1;
            entry.avg_latency = (entry.avg_latency * (entry.total_events - 1) as f64
                + event.latency_ms as f64)
                / entry.total_events as f64;

            // Detect errors and high latency
            if event.level == "ERROR" {
                entry.error_count += 1;
            }
            if event.latency_ms > 1000 {
                // Latency over 1 second
                entry.high_latency_count += 1;
            }

            // Calculate error rate
            entry.error_rate = entry.error_count as f64 / entry.total_events as f64;

            stats
        })
        // Output results to sink
        .sink(sink.clone())
        .await?;

    // Print results
    println!("\nLog Anomaly Detection Statistics:");
    for result in sink.get_data() {
        for (_, stats) in result {
            println!(
                "Service: {}, Error Rate: {:.2}%, Avg Latency: {:.2}ms, Error Count: {}, High Latency Events: {}, Total Events: {}",
                stats.service,
                stats.error_rate * 100.0,
                stats.avg_latency,
                stats.error_count,
                stats.high_latency_count,
                stats.total_events
            );
        }
    }

    Ok(())
}

// Generate sample log events
fn generate_sample_events() -> Vec<LogEvent> {
    let services = vec![
        "api-gateway",
        "user-service",
        "order-service",
        "payment-service",
    ];
    let mut events = Vec::new();
    let start_time = SystemTime::now();

    for i in 0..200 {
        for service in &services {
            // Simulate different error probabilities for services
            let error_prob = match *service {
                "api-gateway" => 0.05,
                "user-service" => 0.02,
                "order-service" => 0.08,
                "payment-service" => 0.03,
                _ => 0.01,
            };

            // Randomly select log level
            let level = if rand_float() < error_prob {
                "ERROR"
            } else if rand_float() < 0.15 {
                "WARN"
            } else {
                "INFO"
            };

            // Simulate latency
            let base_latency = match *service {
                "api-gateway" => 50,
                "user-service" => 100,
                "order-service" => 150,
                "payment-service" => 200,
                _ => 100,
            };

            let latency = base_latency + (rand_float() * 1000.0) as u64;
            let message = format!("Processing request #{i}");

            let event = LogEvent {
                service: service.to_string(),
                level: level.to_string(),
                message,
                latency_ms: latency,
                timestamp: start_time + Duration::from_secs(i as u64 / 2), // One event every 0.5 seconds
            };
            events.push(event);
        }
    }

    events
}

// Generate random float between 0 and 1
fn rand_float() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time cannot be earlier than UNIX epoch")
        .subsec_nanos() as f64;
    (nanos % 1000.0) / 1000.0
}
