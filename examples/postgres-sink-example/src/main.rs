use anyhow::Result;
use fluxus_sinks::{PostgresSink, PostgresType, Sink};
use fluxus_utils::models::Record;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct UserEvent {
    user_id: u64,
    event_type: String,
    page: String,
    duration_ms: u64,
    user_agent: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting PostgreSQL Sink Example");
    info!("Make sure PostgreSQL is running and accessible");
    info!("Create database and table first:");
    info!("  CREATE DATABASE fluxus_test;");
    info!("  CREATE TABLE user_events (id SERIAL PRIMARY KEY, data JSONB, created_at TIMESTAMP);");

    // Create sample user event data
    let user_events = vec![
        UserEvent {
            user_id: 12345,
            event_type: "page_view".to_string(),
            page: "/home".to_string(),
            duration_ms: 2500,
            user_agent: "Mozilla/5.0 Chrome/91.0".to_string(),
        },
        UserEvent {
            user_id: 67890,
            event_type: "button_click".to_string(),
            page: "/products".to_string(),
            duration_ms: 150,
            user_agent: "Mozilla/5.0 Firefox/89.0".to_string(),
        },
        UserEvent {
            user_id: 11111,
            event_type: "form_submit".to_string(),
            page: "/contact".to_string(),
            duration_ms: 5000,
            user_agent: "Mozilla/5.0 Safari/14.1".to_string(),
        },
    ];

    // Test simple PostgreSQL sink (data only)
    test_postgres_simple_sink(&user_events).await?;

    // Test PostgreSQL sink with timestamp column
    test_postgres_with_timestamp(&user_events).await?;

    // Test structured PostgreSQL sink with column mapping
    test_postgres_structured_sink(&user_events).await?;

    info!("PostgreSQL Sink Example completed successfully!");
    Ok(())
}

async fn test_postgres_simple_sink(events: &[UserEvent]) -> Result<()> {
    info!("Testing PostgreSQL simple sink (data only)");

    let mut postgres_sink = PostgresSink::simple_jsonb(
        "host=localhost user=postgres dbname=fluxus_test",
        "user_events",
        "data",
    );

    match postgres_sink.init().await {
        Ok(_) => {
            info!("PostgreSQL simple sink initialized successfully");

            for event in events {
                let record = Record::new(event.clone());
                match postgres_sink.write(record).await {
                    Ok(_) => info!(
                        "Written user event to PostgreSQL: {} on {}",
                        event.event_type, event.page
                    ),
                    Err(e) => warn!("Failed to write to PostgreSQL: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }

            postgres_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to PostgreSQL for simple sink: {}", e),
    }

    Ok(())
}

async fn test_postgres_with_timestamp(events: &[UserEvent]) -> Result<()> {
    info!("Testing PostgreSQL sink with timestamp column");

    let mut postgres_sink = PostgresSink::jsonb_with_timestamp(
        "host=localhost user=postgres dbname=fluxus_test",
        "user_events",
        "data",
        "created_at",
    );

    match postgres_sink.init().await {
        Ok(_) => {
            info!("PostgreSQL timestamp sink initialized successfully");

            for event in events {
                let record = Record::new(event.clone());
                match postgres_sink.write(record).await {
                    Ok(_) => info!(
                        "Written user event with timestamp to PostgreSQL: {} on {}",
                        event.event_type, event.page
                    ),
                    Err(e) => warn!("Failed to write to PostgreSQL with timestamp: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }

            postgres_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to PostgreSQL for timestamp sink: {}", e),
    }

    Ok(())
}

async fn test_postgres_structured_sink(events: &[UserEvent]) -> Result<()> {
    info!("Testing PostgreSQL structured sink with column mapping");
    info!("Create table first:");
    info!("  CREATE TABLE user_analytics (");
    info!("    id SERIAL PRIMARY KEY,");
    info!("    user_id BIGINT NOT NULL,");
    info!("    event_type TEXT NOT NULL,");
    info!("    page_path TEXT,");
    info!("    duration_seconds DOUBLE PRECISION,");
    info!("    browser_type TEXT,");
    info!("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    info!("  );");

    let mut postgres_sink = PostgresSink::<UserEvent>::builder(
        "host=localhost user=postgres dbname=fluxus_test",
        "user_analytics",
    )
    .map_column("user_id", "user_id", PostgresType::BigInt, true)
    .map_column("event_type", "event_type", PostgresType::Text, true)
    .map_column("page_path", "page", PostgresType::Text, false)
    .map_column(
        "duration_seconds",
        "duration_ms",
        PostgresType::Double,
        false,
    )
    .map_column("browser_type", "user_agent", PostgresType::Text, false)
    .with_timestamp("created_at")
    .batch_size(10)
    .build();

    match postgres_sink.init().await {
        Ok(_) => {
            info!("PostgreSQL structured sink initialized successfully");

            for event in events {
                let record = Record::new(event.clone());
                match postgres_sink.write(record).await {
                    Ok(_) => info!(
                        "Written structured user event: {} by user {}",
                        event.event_type, event.user_id
                    ),
                    Err(e) => warn!("Failed to write structured data to PostgreSQL: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }

            // Force flush any remaining batch data
            postgres_sink.flush().await?;
            postgres_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to PostgreSQL for structured sink: {}", e),
    }

    Ok(())
}
