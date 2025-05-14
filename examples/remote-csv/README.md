# Remote CSV Example

This example demonstrates how to use Fluxus for reading and processing remote CSV data streams. It shows how to connect to a remote CSV file and process its contents line by line.

## Features

- Remote CSV file streaming
- URL-based data source
- Line-by-line processing
- Asynchronous data reading
- Source initialization and cleanup

## Running the Example

```bash
cargo run
```

## Implementation Details

- Connects to a remote CSV file via URL
- Initializes a CSV source stream
- Processes records asynchronously
- Demonstrates proper source cleanup
- Handles streaming termination

## Output Example

```
Reading CSV data from: [URL]
Line 1: [CSV record data]
Line 2: [CSV record data]
...
Done!
```

## Dependencies

- fluxus-core
- fluxus-runtime
- fluxus-sources
- tokio