
# Click Stream Analysis Example

This example demonstrates how to use Fluxus to process click - stream data and implement user session analysis. It simulates the browsing behavior of users on an e - commerce website, including the processing of page visits and click events.

## Features

- User session tracking
- Page browsing path analysis
- Session duration statistics
- Event aggregation processing

## Running the Example

```bash
cargo run
```

## Implementation Details

- Use session windows (30 - second timeout) to group user behaviors
- Filter and process page visit events
- Calculate session duration and total number of events
- Record the user's page visit sequence

## Output Example

```
Click stream analysis results:

Session window results:
User user1: 4 events over 30s, Pages: home -> products -> cart -> checkout
```

## Dependencies

- fluxus - core
- fluxus - runtime
- fluxus - api
- tokio
- anyhow
