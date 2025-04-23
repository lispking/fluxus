# Temperature Analysis Example

This example demonstrates how to use Fluxus to monitor real-time temperature data, analyze temperature change trends, and detect abnormal temperature values.

## Features

- Monitor real-time temperature data.
- Analyze temperature change trends.
- Detect abnormal temperature values.

## Running the Example

## Implementation Details

- Use a streaming processing framework to process temperature data.
- Filter and aggregate real-time data.
- Apply a threshold detection algorithm to identify abnormal temperatures.

## Output Example

```
Temperature analysis results:

Window results:
Sensor sensor3: 100 readings, Avg: 25.0°C, Min: 22.0°C, Max: 28.0°C, Avg Humidity: 60.0%
Sensor sensor1: 100 readings, Avg: 20.4°C, Min: 18.0°C, Max: 22.0°C, Avg Humidity: 49.8%
Sensor sensor2: 100 readings, Avg: 26.9°C, Min: 22.0°C, Max: 31.9°C, Avg Humidity: 64.9%
```

## Dependencies

- fluxus - core
- fluxus - runtime
- fluxus - api
- tokio
- anyhow
