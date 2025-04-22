use fluxus_api::{DataStream, io::{CollectionSource, CollectionSink}};
use fluxus_core::WindowConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::time::{SystemTime, Duration};

#[derive(Clone)]
pub struct SensorReading {
    sensor_id: String,
    temperature: f64,
    timestamp: SystemTime,
}

#[derive(Clone)]
pub struct AggregatedReading {
    sensor_id: String,
    avg_temperature: f64,
    min_temperature: f64,
    max_temperature: f64,
    reading_count: usize,
}

pub async fn run() -> Result<()> {
    // Generate sample temperature readings
    let readings = generate_sample_readings();
    let source = CollectionSource::new(readings);
    let sink = CollectionSink::new();

    // Build and execute the streaming pipeline
    DataStream::new(source)
        // Group by sensor_id
        .map(|reading| {
            (
                reading.sensor_id.clone(),
                (reading.temperature, reading.timestamp)
            )
        })
        // Create 10-second tumbling windows
        .window(WindowConfig::Tumbling { size_ms: 10000 })
        // Aggregate temperatures in each window
        .aggregate(
            HashMap::new(),
            |mut stats, (sensor_id, (temp, _))| {
                let entry = stats.entry(sensor_id.clone()).or_insert_with(|| {
                    AggregatedReading {
                        sensor_id: String::new(),
                        avg_temperature: 0.0,
                        min_temperature: f64::MAX,
                        max_temperature: f64::MIN,
                        reading_count: 0,
                    }
                });
                
                entry.sensor_id = sensor_id;
                entry.min_temperature = entry.min_temperature.min(temp);
                entry.max_temperature = entry.max_temperature.max(temp);
                entry.avg_temperature = (entry.avg_temperature * entry.reading_count as f64 + temp) 
                    / (entry.reading_count + 1) as f64;
                entry.reading_count += 1;
                
                stats
            },
        )
        .sink(sink.clone())
        .await?;

    // Print results
    println!("\nTemperature analysis results:");
    for window_stats in sink.get_data() {
        println!("\nWindow results:");
        for (_, stats) in window_stats {
            println!(
                "Sensor {}: {} readings, Avg: {:.1}°C, Min: {:.1}°C, Max: {:.1}°C",
                stats.sensor_id,
                stats.reading_count,
                stats.avg_temperature,
                stats.min_temperature,
                stats.max_temperature
            );
        }
    }

    Ok(())
}

// Helper function to generate sample data
fn generate_sample_readings() -> Vec<SensorReading> {
    let start_time = SystemTime::now();
    let mut readings = Vec::new();

    for i in 0..100 {
        let timestamp = start_time + Duration::from_secs(i as u64 / 10);
        
        // Sensor 1: Normal temperature variations
        readings.push(SensorReading {
            sensor_id: "sensor1".to_string(),
            temperature: 20.0 + (i as f64 / 10.0).sin() * 2.0,
            timestamp,
        });

        // Sensor 2: Gradually increasing temperature
        readings.push(SensorReading {
            sensor_id: "sensor2".to_string(),
            temperature: 22.0 + i as f64 * 0.1,
            timestamp,
        });

        // Sensor 3: Random fluctuations
        readings.push(SensorReading {
            sensor_id: "sensor3".to_string(),
            temperature: 25.0 + (i as f64 * 0.7).cos() * 3.0,
            timestamp,
        });
    }

    readings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_temperature_processing() -> Result<()> {
        let readings = vec![
            SensorReading {
                sensor_id: "test1".to_string(),
                temperature: 20.0,
                timestamp: SystemTime::now(),
            },
            SensorReading {
                sensor_id: "test1".to_string(),
                temperature: 22.0,
                timestamp: SystemTime::now(),
            },
        ];

        let source = CollectionSource::new(readings);
        let sink: CollectionSink<HashMap<String, AggregatedReading>> = CollectionSink::new();

        DataStream::new(source)
            .map(|reading| (reading.sensor_id.clone(), (reading.temperature, reading.timestamp)))
            .window(WindowConfig::Tumbling { size_ms: 1000 })
            .aggregate(
                HashMap::new(),
                |mut stats, (sensor_id, (temp, _))| {
                    let entry = stats.entry(sensor_id.clone()).or_insert_with(|| {
                        AggregatedReading {
                            sensor_id: String::new(),
                            avg_temperature: 0.0,
                            min_temperature: f64::MAX,
                            max_temperature: f64::MIN,
                            reading_count: 0,
                        }
                    });
                    
                    entry.sensor_id = sensor_id;
                    entry.min_temperature = entry.min_temperature.min(temp);
                    entry.max_temperature = entry.max_temperature.max(temp);
                    entry.avg_temperature = (entry.avg_temperature * entry.reading_count as f64 + temp) 
                        / (entry.reading_count + 1) as f64;
                    entry.reading_count += 1;
                    
                    stats
                },
            )
            .sink(sink.clone())
            .await?;

        let results = sink.get_data();
        assert!(!results.is_empty());
        
        if let Some(window_stats) = results.first() {
            let stats = window_stats.get("test1").unwrap();
            assert_eq!(stats.reading_count, 2);
            assert_eq!(stats.min_temperature, 20.0);
            assert_eq!(stats.max_temperature, 22.0);
            assert_eq!(stats.avg_temperature, 21.0);
        }

        Ok(())
    }
}