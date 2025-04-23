# Fluxus Core

Core implementations and data structures for the Fluxus stream processing engine.

## Overview

This crate provides the fundamental building blocks and implementations for the Fluxus stream processing engine:

- Window implementations
- State management
- Data partitioning
- Runtime configurations
- Core data structures

## Key Components

### Windows

Core window implementations:
- `TumblingWindow` - Fixed-size, non-overlapping windows
- `SlidingWindow` - Overlapping windows with slide interval
- `SessionWindow` - Dynamic windows based on event timing

### State Management

State handling for stream operations:
- In-memory state storage
- State backends
- Checkpointing (planned)

### Partitioning

Data partitioning strategies:
- Key-based partitioning
- Round-robin partitioning
- Custom partitioners

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
fluxus-core = "0.1"
```

This crate is usually not used directly but through the `fluxus-api` crate.