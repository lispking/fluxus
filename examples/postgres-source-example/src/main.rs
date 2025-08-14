use anyhow::Result;
use fluxus_sources::{PostgresSource, Source};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LogEntry {
    id: i64,
    level: String,
    message: String,
    service: String,
    timestamp: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting PostgreSQL Source Example");
    info!("Make sure PostgreSQL is running and accessible");
    info!("Create database and sample data first:");
    info!("  CREATE DATABASE fluxus_test;");
    info!("  CREATE TABLE log_entries (");
    info!("    id SERIAL PRIMARY KEY,");
    info!("    level TEXT NOT NULL,");
    info!("    message TEXT NOT NULL,");
    info!("    service TEXT NOT NULL,");
    info!("    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    info!("  );");
    info!("  INSERT INTO log_entries (level, message, service) VALUES");
    info!("    ('INFO', 'Application started', 'web-server'),");
    info!("    ('ERROR', 'Database connection failed', 'api-service'),");
    info!("    ('WARN', 'High memory usage detected', 'worker'),");
    info!("    ('INFO', 'User logged in', 'auth-service');");

    // Test single query source
    test_single_query_source().await?;
    
    // Test polling source (commented out to avoid infinite loop in example)
    // test_polling_source().await?;

    info!("PostgreSQL Source Example completed successfully!");
    Ok(())
}

async fn test_single_query_source() -> Result<()> {
    info!("Testing PostgreSQL single query source");
    
    let mut source = PostgresSource::<LogEntry>::query(
        "host=localhost user=postgres dbname=fluxus_test",
        "SELECT id, level, message, service, timestamp::text as timestamp FROM log_entries ORDER BY id"
    );

    match source.init().await {
        Ok(_) => {
            info!("PostgreSQL source initialized successfully");
            
            let mut count = 0;
            loop {
                match source.next().await {
                    Ok(Some(record)) => {
                        count += 1;
                        info!("Read log entry {}: [{}] {} from {} at {}", 
                            record.data.id,
                            record.data.level,
                            record.data.message,
                            record.data.service,
                            record.data.timestamp
                        );
                    }
                    Ok(None) => {
                        info!("No more data available");
                        break;
                    }
                    Err(e) => {
                        warn!("Failed to read from PostgreSQL: {}", e);
                        break;
                    }
                }
            }
            
            info!("Read {} log entries total", count);
            source.close().await?;
        }
        Err(e) => warn!("Failed to connect to PostgreSQL: {}", e),
    }
    
    Ok(())
}

#[allow(dead_code)]
async fn test_polling_source() -> Result<()> {
    info!("Testing PostgreSQL polling source");
    info!("This will poll for new entries every 5 seconds");
    
    let mut source = PostgresSource::<LogEntry>::polling(
        "host=localhost user=postgres dbname=fluxus_test",
        "SELECT id, level, message, service, timestamp::text as timestamp FROM log_entries WHERE id > COALESCE($1::bigint, 0) ORDER BY id",
        Duration::from_secs(5)
    );

    match source.init().await {
        Ok(_) => {
            info!("PostgreSQL polling source initialized successfully");
            
            let mut count = 0;
            let max_polls = 3; // Limit for example
            
            while count < max_polls {
                match source.next().await {
                    Ok(Some(record)) => {
                        info!("Polled log entry {}: [{}] {} from {}", 
                            record.data.id,
                            record.data.level,
                            record.data.message,
                            record.data.service
                        );
                    }
                    Ok(None) => {
                        info!("No new data in this poll cycle");
                    }
                    Err(e) => {
                        warn!("Failed to poll PostgreSQL: {}", e);
                        break;
                    }
                }
                count += 1;
            }
            
            source.close().await?;
        }
        Err(e) => warn!("Failed to connect to PostgreSQL for polling: {}", e),
    }
    
    Ok(())
}
