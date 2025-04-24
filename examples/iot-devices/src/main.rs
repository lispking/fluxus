use anyhow::Result;
use fluxus::api::{
    DataStream,
    io::{CollectionSink, CollectionSource},
};
use fluxus::utils::window::WindowConfig;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

#[derive(Clone)]
pub struct IoTData {
    device_id: String,
    device_type: String,
    value: f64,
    battery_level: u8,
    signal_strength: i32,
    timestamp: SystemTime,
}

#[derive(Clone)]
pub struct DeviceStats {
    device_id: String,
    device_type: String,
    avg_value: f64,
    min_battery: u8,
    avg_signal: i32,
    alert_count: u32,
    last_update: SystemTime,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Generate sample IoT device data
    let iot_data = generate_sample_data();
    let source = CollectionSource::new(iot_data);
    let sink = CollectionSink::new();

    // Build and execute stream processing pipeline
    DataStream::new(source)
        // Group by device ID
        .map(|data| (data.device_id.clone(), data))
        // Create 2-minute sliding window with 30-second slide
        .window(WindowConfig::sliding(
            Duration::from_secs(120), // 2 minutes
            Duration::from_secs(30),  // 30 seconds
        ))
        // Aggregate device statistics
        .aggregate(HashMap::new(), |mut stats, (device_id, data)| {
            let entry = stats
                .entry(device_id.clone())
                .or_insert_with(|| DeviceStats {
                    device_id,
                    device_type: data.device_type.clone(),
                    avg_value: 0.0,
                    min_battery: data.battery_level,
                    avg_signal: 0,
                    alert_count: 0,
                    last_update: data.timestamp,
                });

            // Update statistics
            entry.avg_value = (entry.avg_value + data.value) / 2.0;
            entry.min_battery = entry.min_battery.min(data.battery_level);
            entry.avg_signal = (entry.avg_signal + data.signal_strength) / 2;
            entry.last_update = data.timestamp;

            // Check alert conditions
            if data.battery_level < 20 || data.signal_strength < -90 {
                entry.alert_count += 1;
            }

            stats
        })
        // Output results to sink
        .sink(sink.clone())
        .await?;

    // Print results
    println!("\nIoT Device Statistics:");
    for result in sink.get_data() {
        for (_, stats) in result {
            println!(
                "Device ID: {}, Type: {}, Average Value: {:.2}, Min Battery: {}%, Average Signal: {}dBm, Alert Count: {}",
                stats.device_id,
                stats.device_type,
                stats.avg_value,
                stats.min_battery,
                stats.avg_signal,
                stats.alert_count
            );
        }
    }

    Ok(())
}

// Generate sample IoT device data
fn generate_sample_data() -> Vec<IoTData> {
    let device_types = [
        "Temperature Sensor",
        "Humidity Sensor",
        "Pressure Sensor",
        "Light Sensor",
    ];
    let mut data = Vec::new();
    let start_time = SystemTime::now();

    for i in 0..100 {
        for j in 1..=5 {
            let device_type = device_types[j % device_types.len()];
            let base_value = match device_type {
                "Temperature Sensor" => 25.0,
                "Humidity Sensor" => 60.0,
                "Pressure Sensor" => 1013.0,
                "Light Sensor" => 500.0,
                _ => 0.0,
            };

            // Simulate data fluctuation
            let value_variation = (i as f64 * 0.1).sin() * 5.0;
            let battery_drain = (i / 20) as u8; // Simulate battery consumption

            let reading = IoTData {
                device_id: format!("DEV_{:03}", j),
                device_type: device_type.to_string(),
                value: base_value + value_variation,
                battery_level: 100 - battery_drain,
                signal_strength: -70 - (i % 30), // Simulate signal strength fluctuation
                timestamp: start_time + Duration::from_secs(i as u64 * 15), // One data point every 15 seconds
            };
            data.push(reading);
        }
    }

    data
}
