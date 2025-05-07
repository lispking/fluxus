# Fluxus Examples

A collection of example applications demonstrating the usage of the Fluxus stream processing engine.

## Available Examples

### 1. Word Count (`word-count`)

Demonstrates basic stream processing with tumbling windows:
- Splits text into words
- Counts word frequencies in time windows
- Shows parallel processing capabilities

```bash
cargo run --example word-count
```

### 2. Temperature Sensor Analysis (`temperature-sensor`)

Shows how to process IoT sensor data:
- Processes multiple sensor readings
- Calculates min/max/average temperatures
- Uses sliding windows for continuous monitoring

```bash
cargo run --example temperature-sensor
```

### 3. Click Stream Analysis (`click-stream`)

Demonstrates session window usage for user behavior analysis:
- Tracks user navigation patterns
- Groups events into sessions
- Analyzes user engagement metrics

```bash
cargo run --example click-stream
```

### 4. Network Log Analysis (`network-log`)

Shows advanced stream processing features:
- Processes HTTP access logs
- Calculates request statistics
- Uses sliding windows with custom aggregations

```bash
cargo run --example network-log
```

### 5. IoT Device Analysis (`iot-devices`)

Demonstrates how to process various IoT device data:
- Processes sensor data from different devices
- Calculates device status statistics
- Uses tumbling windows for real-time monitoring

```bash
cargo run --example iot-devices
```

### 6. Log Anomaly Detection (`log-anomaly`)

Demonstrates log anomaly detection capabilities:
- Processes system log data
- Detects abnormal log patterns
- Uses custom windows for anomaly analysis

```bash
cargo run --example log-anomaly
```

### 7. Stock Market Analysis (`stock-market`)

Demonstrates stock market data processing:
- Processes real-time stock price data
- Calculates stock price indicators
- Uses session windows to analyze trading patterns

```bash
cargo run --example stock-market
```

## Example Structure

Each example follows a similar pattern:
1. Define data structures
2. Create a data source
3. Build processing pipeline
4. Configure windows
5. Define aggregations
6. Output results

## Learning Path

We recommend going through the examples in this order:
1. Word Count - Basic concepts
2. Temperature Sensor - Time-based windows
3. Click Stream - Session windows
4. Network Log - Advanced features
5. IoT Devices - Multiple data sources
6. Log Anomaly - Custom windows
7. Stock Market - Real-time monitoring
8. [GitHub Archive](https://github.com/fluxus-labs/fluxus-source-gharchive/tree/main/examples) - Count event type from GitHub archive file