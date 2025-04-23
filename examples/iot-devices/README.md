
# IoT Device Monitoring Example

This example demonstrates how to use Fluxus to process and analyze IoT device data streams. It implements real - time monitoring of multiple IoT devices, including data aggregation, statistical analysis, and alarm detection.

## Features

- Multi - device data stream processing
- Sliding window statistical analysis
- Device status monitoring
- Low battery and weak signal alarms
- Real - time data aggregation

## Running the Example

```bash
cargo run
```

## Implementation Details

- Use a 2 - minute sliding window with a 30 - second sliding interval
- Calculate the average value of device data
- Monitor battery level and signal strength
- Count alarm events
- Track the latest update time of devices

## Output Example

```
IoT Device Statistics:
Device ID: DEV_001, Type: Temperature Sensor, Average Value: 25.50, Min Battery: 80%, Average Signal: -85dBm, Alert Count: 2
```

## Dependencies

- fluxus - core
- fluxus - runtime
- fluxus - api
- tokio
- anyhow
