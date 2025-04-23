
# Log Anomaly Detection Example

This example demonstrates how to use Fluxus for real - time log anomaly detection. It analyzes the log streams of multiple microservices to detect error rates and performance anomalies.

## Features

- Multi - service log stream processing
- Error rate statistics
- Latency anomaly detection
- Real - time statistical analysis
- Sliding window aggregation

## Running the Example

```bash
cargo run
```

## Implementation Details

- Use a 1 - minute sliding window with a 10 - second sliding interval.
- Group and count by service name.
- Calculate the error rate and average latency.
- Detect high - latency events (>1 second).
- Update service status statistics in real - time.

## Output Example

```
Log Anomaly Detection Statistics:
Service: api - gateway, Error Rate: 5.00%, Avg Latency: 150.25ms, Error Count: 10, High Latency Events: 5, Total Events: 200
```

## Dependencies

- fluxus - core
- fluxus - runtime
- fluxus - api
- tokio
- anyhow
