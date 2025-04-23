
# Network Log Analysis Example

This example demonstrates how to use Fluxus for real - time network log analysis. It implements real - time processing of HTTP request logs, including request statistics, error rate analysis, and response size monitoring.

## Features

- Real - time processing of HTTP request logs
- Grouped statistics by path
- Error rate monitoring
- Response size analysis
- Sliding window aggregation

## Running the Example

```bash
cargo run
```

## Implementation Details

- Use a 60 - second sliding window with a 10 - second sliding interval.
- Group and count requests by API path.
- Calculate the number of requests and errors for each path.
- Monitor changes in response size.
- Calculate the error rate in real - time.

## Output Example

```
Network log analysis results:
Path: /api/users
  Requests: 50
  Errors: 5
  Avg Size: 1250.50 bytes
  Error Rate: 10.0%
```

## Dependencies

- fluxus - core
- fluxus - runtime
- fluxus - api
- tokio
