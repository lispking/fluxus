use anyhow::Result;
use fluxus::api::{
    DataStream,
    io::{CollectionSink, CollectionSource},
};
use fluxus::utils::window::WindowConfig;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

#[derive(Clone)]
pub struct SensorReading {
    sensor_id: String,
    temperature: f64,
    humidity: f64,
    timestamp: SystemTime,
}

#[derive(Clone)]
pub struct SensorStats {
    sensor_id: String,
    avg_temperature: f64,
    avg_humidity: f64,
    min_temperature: f64,
    max_temperature: f64,
    reading_count: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
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
                (reading.temperature, reading.humidity, reading.timestamp),
            )
        })
        // Create 10-second tumbling windows
        .window(WindowConfig::tumbling(Duration::from_millis(10000)))
        // Aggregate temperatures in each window
        .aggregate(
            HashMap::new(),
            |mut stats, (sensor_id, (temp, humidity, _))| {
                let entry = stats
                    .entry(sensor_id.clone())
                    .or_insert_with(|| SensorStats {
                        sensor_id: String::new(),
                        avg_temperature: 0.0,
                        avg_humidity: 0.0,
                        min_temperature: f64::MAX,
                        max_temperature: f64::MIN,
                        reading_count: 0,
                    });

                entry.sensor_id = sensor_id;
                entry.min_temperature = entry.min_temperature.min(temp);
                entry.max_temperature = entry.max_temperature.max(temp);
                entry.avg_temperature = (entry.avg_temperature * entry.reading_count as f64 + temp)
                    / (entry.reading_count + 1) as f64;
                entry.avg_humidity = (entry.avg_humidity * entry.reading_count as f64 + humidity)
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
                "Sensor {}: {} readings, Avg: {:.1}°C, Min: {:.1}°C, Max: {:.1}°C, Avg Humidity: {:.1}%",
                stats.sensor_id,
                stats.reading_count,
                stats.avg_temperature,
                stats.min_temperature,
                stats.max_temperature,
                stats.avg_humidity,
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
            humidity: 50.0 + (i as f64 / 10.0).cos() * 5.0,
            timestamp,
        });

        // Sensor 2: Gradually increasing temperature
        readings.push(SensorReading {
            sensor_id: "sensor2".to_string(),
            temperature: 22.0 + i as f64 * 0.1,
            humidity: 55.0 + i as f64 * 0.2,
            timestamp,
        });

        // Sensor 3: Random fluctuations
        readings.push(SensorReading {
            sensor_id: "sensor3".to_string(),
            temperature: 25.0 + (i as f64 * 0.7).cos() * 3.0,
            humidity: 60.0 + (i as f64 * 0.5).sin() * 4.0,
            timestamp,
        });
    }

    readings
}
