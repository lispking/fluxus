# Fluxus API

Core API definitions and interfaces for the Fluxus stream processing engine.

## Overview

This crate provides the public API for building stream processing applications with Fluxus. It includes:

- `DataStream` - The main abstraction for working with data streams
- Source and Sink interfaces
- Stream operations (map, filter, aggregate, etc.)
- Window configurations
- I/O utilities

## Key Components

### DataStream

The `DataStream` type is the main entry point for building stream processing pipelines:

```rust
DataStream::new(source)
    .map(|x| x * 2)
    .filter(|x| x > 0)
    .window(WindowConfig::Tumbling { size_ms: 1000 })
    .aggregate(initial_state, |state, value| /* aggregation logic */)
    .sink(sink)
```

### Windows

Supported window types:
- Tumbling Windows - Fixed-size, non-overlapping windows
- Sliding Windows - Fixed-size windows that slide by a specified interval
- Session Windows - Dynamic windows based on activity timeouts

### I/O

Pre-built source and sink implementations:
- `CollectionSource` - Create a stream from a collection
- `CollectionSink` - Collect stream results into a collection
- Additional I/O implementations for files, networks, etc.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
fluxus-api = "0.2"
```

See the `fluxus-examples` crate for complete usage examples.