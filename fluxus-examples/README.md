# Fluxus Examples

A collection of example applications demonstrating the usage of the Fluxus stream processing engine.

## Available Examples

### 1. Word Count (`word_count.rs`)
Demonstrates basic stream processing with tumbling windows:
- Splits text into words
- Counts word frequencies in time windows
- Shows parallel processing capabilities
```bash
cargo run -- word-count
```

### 2. Temperature Sensor Analysis (`temperature_sensor.rs`)
Shows how to process IoT sensor data:
- Processes multiple sensor readings
- Calculates min/max/average temperatures
- Uses sliding windows for continuous monitoring
```bash
cargo run -- temperature
```

### 3. Click Stream Analysis (`click_stream.rs`)
Demonstrates session window usage for user behavior analysis:
- Tracks user navigation patterns
- Groups events into sessions
- Analyzes user engagement metrics
```bash
cargo run -- click-stream
```

### 4. Network Log Analysis (`network_log.rs`)
Shows advanced stream processing features:
- Processes HTTP access logs
- Calculates request statistics
- Uses sliding windows with custom aggregations
```bash
cargo run -- network-log
```

## Running the Examples

### View Available Options
```bash
cargo run -- --help
```

### Running a Specific Example
```bash
cargo run -- <example-name>
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

## Testing

Each example includes unit tests demonstrating the expected behavior:
```bash
cargo test
```