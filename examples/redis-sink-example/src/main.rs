use anyhow::Result;
use fluxus_sinks::{RedisSink, Sink};
use fluxus_utils::models::Record;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SensorData {
    sensor_id: String,
    temperature: f64,
    humidity: f64,
    location: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting Redis Sink Example");
    info!("Make sure Redis is running on localhost:6379");

    // Create sample sensor data
    let sensor_readings = vec![
        SensorData {
            sensor_id: "temp_001".to_string(),
            temperature: 23.5,
            humidity: 45.2,
            location: "Office".to_string(),
        },
        SensorData {
            sensor_id: "temp_002".to_string(),
            temperature: 25.1,
            humidity: 52.8,
            location: "Warehouse".to_string(),
        },
        SensorData {
            sensor_id: "temp_003".to_string(),
            temperature: 21.8,
            humidity: 38.9,
            location: "Lab".to_string(),
        },
    ];

    // Test different Redis operations
    test_redis_set_operation(&sensor_readings).await?;
    test_redis_hset_operation(&sensor_readings).await?;
    test_redis_list_operations(&sensor_readings).await?;
    test_redis_stream_operation(&sensor_readings).await?;

    info!("Redis Sink Example completed successfully!");
    Ok(())
}

async fn test_redis_set_operation(data: &[SensorData]) -> Result<()> {
    info!("Testing Redis SET operation");
    
    let mut redis_sink = RedisSink::set(
        "redis://127.0.0.1:6379".to_string(),
        "sensor_data"
    );

    match redis_sink.init().await {
        Ok(_) => {
            info!("Redis SET sink initialized successfully");
            
            for reading in data {
                let record = Record::new(reading.clone());
                match redis_sink.write(record).await {
                    Ok(_) => info!("Written sensor data to Redis SET: {}", reading.sensor_id),
                    Err(e) => warn!("Failed to write to Redis SET: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }
            
            redis_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to Redis for SET operation: {}", e),
    }
    
    Ok(())
}

async fn test_redis_hset_operation(data: &[SensorData]) -> Result<()> {
    info!("Testing Redis HSET operation");
    
    let mut redis_sink = RedisSink::hset(
        "redis://127.0.0.1:6379".to_string(),
        "sensor_hash",
        "reading"
    );

    match redis_sink.init().await {
        Ok(_) => {
            info!("Redis HSET sink initialized successfully");
            
            for reading in data {
                let record = Record::new(reading.clone());
                match redis_sink.write(record).await {
                    Ok(_) => info!("Written sensor data to Redis HSET: {}", reading.sensor_id),
                    Err(e) => warn!("Failed to write to Redis HSET: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }
            
            redis_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to Redis for HSET operation: {}", e),
    }
    
    Ok(())
}

async fn test_redis_list_operations(data: &[SensorData]) -> Result<()> {
    info!("Testing Redis LIST operations");
    
    // Test LPUSH
    let mut lpush_sink = RedisSink::lpush(
        "redis://127.0.0.1:6379".to_string(),
        "sensor_list_left"
    );

    match lpush_sink.init().await {
        Ok(_) => {
            info!("Redis LPUSH sink initialized successfully");
            
            for reading in data {
                let record = Record::new(reading.clone());
                match lpush_sink.write(record).await {
                    Ok(_) => info!("Written sensor data to Redis LPUSH: {}", reading.sensor_id),
                    Err(e) => warn!("Failed to write to Redis LPUSH: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }
            
            lpush_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to Redis for LPUSH operation: {}", e),
    }

    // Test RPUSH
    let mut rpush_sink = RedisSink::rpush(
        "redis://127.0.0.1:6379".to_string(),
        "sensor_list_right"
    );

    match rpush_sink.init().await {
        Ok(_) => {
            info!("Redis RPUSH sink initialized successfully");
            
            for reading in data {
                let record = Record::new(reading.clone());
                match rpush_sink.write(record).await {
                    Ok(_) => info!("Written sensor data to Redis RPUSH: {}", reading.sensor_id),
                    Err(e) => warn!("Failed to write to Redis RPUSH: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }
            
            rpush_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to Redis for RPUSH operation: {}", e),
    }
    
    Ok(())
}

async fn test_redis_stream_operation(data: &[SensorData]) -> Result<()> {
    info!("Testing Redis Stream operation (XADD)");
    
    let mut redis_sink = RedisSink::stream(
        "redis://127.0.0.1:6379".to_string(),
        "sensor_events"
    );

    match redis_sink.init().await {
        Ok(_) => {
            info!("Redis Stream sink initialized successfully");
            
            for reading in data {
                let record = Record::new(reading.clone());
                match redis_sink.write(record).await {
                    Ok(_) => info!("Written sensor data to Redis Stream: {} at {}", 
                        reading.sensor_id, reading.location),
                    Err(e) => warn!("Failed to write to Redis Stream: {}", e),
                }
                time::sleep(Duration::from_millis(100)).await;
            }
            
            redis_sink.close().await?;
        }
        Err(e) => warn!("Failed to connect to Redis for Stream operation: {}", e),
    }
    
    Ok(())
}
