use anyhow::Result;
use fluxus_sinks::{PostgresSink, PostgresType, RedisSink, Sink};
use fluxus_sources::{PostgresSource, RedisSource, Source};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct UserEvent {
    id: Option<i64>,
    user_id: i64,
    event_type: String,
    page: String,
    timestamp: String,
    metadata: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting Complete Data Pipeline Example");
    info!("This demonstrates: PostgreSQL → Redis Queue → PostgreSQL Analytics");
    
    info!("Prerequisites:");
    info!("1. PostgreSQL running on localhost:5432");
    info!("2. Redis running on localhost:6379");
    info!("3. Create source table in PostgreSQL:");
    info!("   CREATE TABLE raw_events (");
    info!("     id SERIAL PRIMARY KEY,");
    info!("     user_id BIGINT,");
    info!("     event_type TEXT,");
    info!("     page TEXT,");
    info!("     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,");
    info!("     metadata JSONB");
    info!("   );");
    info!("4. Create analytics table:");
    info!("   CREATE TABLE user_analytics (");
    info!("     id SERIAL PRIMARY KEY,");
    info!("     user_id BIGINT,");
    info!("     event_type TEXT,");
    info!("     page_path TEXT,");
    info!("     event_timestamp TEXT,");
    info!("     processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    info!("   );");

    // Step 1: Read from PostgreSQL and push to Redis queue
    info!("\n=== Step 1: PostgreSQL → Redis Queue ===");
    producer_pipeline().await?;

    // Wait a bit for data to be queued
    time::sleep(Duration::from_secs(2)).await;

    // Step 2: Consume from Redis and write to PostgreSQL analytics
    info!("\n=== Step 2: Redis Queue → PostgreSQL Analytics ===");
    consumer_pipeline().await?;

    info!("\nComplete Data Pipeline Example finished!");
    Ok(())
}

async fn producer_pipeline() -> Result<()> {
    info!("Starting producer: Reading from raw_events and pushing to Redis queue");

    // Create PostgreSQL source
    let mut postgres_source = PostgresSource::<UserEvent>::query(
        "host=localhost user=postgres dbname=fluxus_test",
        r#"
        SELECT 
            id, 
            user_id, 
            event_type, 
            page,
            timestamp::text as timestamp,
            metadata
        FROM raw_events 
        ORDER BY id
        "#
    );

    // Create Redis sink for queueing
    let mut redis_sink = RedisSink::stream(
        "redis://127.0.0.1:6379".to_string(),
        "user_events_queue"
    );

    // Initialize components
    match postgres_source.init().await {
        Ok(_) => info!("✓ PostgreSQL source initialized"),
        Err(e) => {
            warn!("✗ Failed to initialize PostgreSQL source: {}", e);
            return Ok(());
        }
    }

    match redis_sink.init().await {
        Ok(_) => info!("✓ Redis sink initialized"),
        Err(e) => {
            warn!("✗ Failed to initialize Redis sink: {}", e);
            return Ok(());
        }
    }

    // Process data pipeline
    let mut processed_count = 0;
    loop {
        match postgres_source.next().await {
            Ok(Some(record)) => {
                // Process the event (you could add transformation logic here)
                info!("Processing event: {} - {} on {}", 
                    record.data.user_id, 
                    record.data.event_type,
                    record.data.page
                );

                // Push to Redis queue
                match redis_sink.write(record).await {
                    Ok(_) => {
                        processed_count += 1;
                        info!("✓ Queued event #{}", processed_count);
                    }
                    Err(e) => warn!("✗ Failed to queue event: {}", e),
                }
            }
            Ok(None) => {
                info!("✓ No more events to process");
                break;
            }
            Err(e) => {
                warn!("✗ Error reading from PostgreSQL: {}", e);
                break;
            }
        }
    }

    // Cleanup
    postgres_source.close().await?;
    redis_sink.close().await?;

    info!("Producer pipeline completed. Processed {} events", processed_count);
    Ok(())
}

async fn consumer_pipeline() -> Result<()> {
    info!("Starting consumer: Reading from Redis queue and writing to analytics table");

    // Create Redis source for consuming queue
    let mut redis_source = RedisSource::<UserEvent>::stream(
        "redis://127.0.0.1:6379".to_string(),
        "user_events_queue",
        5, // 5 second timeout
        Some("0") // Start from beginning
    );

    // Create structured PostgreSQL sink for analytics
    let mut analytics_sink = PostgresSink::<UserEvent>::builder(
        "host=localhost user=postgres dbname=fluxus_test",
        "user_analytics"
    )
    .map_column("user_id", "user_id", PostgresType::BigInt, true)
    .map_column("event_type", "event_type", PostgresType::Text, true)
    .map_column("page_path", "page", PostgresType::Text, true)
    .map_column("event_timestamp", "timestamp", PostgresType::Text, true)
    .batch_size(5) // Batch for better performance
    .build();

    // Initialize components
    match redis_source.init().await {
        Ok(_) => info!("✓ Redis source initialized"),
        Err(e) => {
            warn!("✗ Failed to initialize Redis source: {}", e);
            return Ok(());
        }
    }

    match analytics_sink.init().await {
        Ok(_) => info!("✓ PostgreSQL analytics sink initialized"),
        Err(e) => {
            warn!("✗ Failed to initialize PostgreSQL sink: {}", e);
            return Ok(());
        }
    }

    // Process data pipeline
    let mut processed_count = 0;
    let max_events = 10; // Limit for example

    while processed_count < max_events {
        match redis_source.next().await {
            Ok(Some(record)) => {
                info!("Consuming event: {} - {} on {}", 
                    record.data.user_id, 
                    record.data.event_type,
                    record.data.page
                );

                // Write to analytics table
                match analytics_sink.write(record).await {
                    Ok(_) => {
                        processed_count += 1;
                        info!("✓ Stored event #{} in analytics", processed_count);
                    }
                    Err(e) => warn!("✗ Failed to store event: {}", e),
                }
            }
            Ok(None) => {
                info!("No more events in queue (timeout)");
                break;
            }
            Err(e) => {
                warn!("✗ Error reading from Redis: {}", e);
                break;
            }
        }
    }

    // Ensure all batched data is written
    analytics_sink.flush().await?;

    // Cleanup
    redis_source.close().await?;
    analytics_sink.close().await?;

    info!("Consumer pipeline completed. Processed {} events", processed_count);
    Ok(())
}
