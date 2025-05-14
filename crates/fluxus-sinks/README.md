# Fluxus Sinks

Sink components for the Fluxus stream processing engine.

## Overview

This crate provides various sink implementations for the Fluxus stream processing engine, allowing processed data to be output to different destinations.

### Key Sinks
- `BufferedSink` - Buffered output for efficient writes.
- `ConsoleSink` - Output data to the console for debugging.
- `DummySink` - A placeholder sink for testing.
- `FileSink` - Write data to files.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
fluxus-sinks = "0.2"
```