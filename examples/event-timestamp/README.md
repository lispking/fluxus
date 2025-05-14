# Event Timestamp Example

This example demonstrates how to use Fluxus for processing timestamped event streams with windowing operations. It shows how to count events within specific time windows and aggregate the results.

## Features

- Event stream processing with timestamps
- Tumbling window implementation
- Event counting and aggregation
- Time-based event analysis
- Real-time event processing

## Running the Example

```bash
cargo run
```

## Implementation Details

- Uses tumbling windows with 1-millisecond duration
- Processes various event types (login, click, purchase)
- Groups and counts events by type and timestamp
- Demonstrates window-based aggregation
- Sorts and displays results by event count

## Output Example

```
Event counts by timestamp:

Time window results:
  ("click", timestamp): 3
  ("login", timestamp): 1
  ("purchase", timestamp): 1
```

## Dependencies

- fluxus-core
- fluxus-runtime
- fluxus-api
- tokio
- anyhow