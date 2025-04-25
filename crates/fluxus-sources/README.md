# Fluxus Sources

Source components for the Fluxus stream processing engine.

## Overview

This crate provides various source implementations for the Fluxus stream processing engine, allowing data to be ingested from different sources.

### Key Sources
- `CsvSource` - Read data from CSV files.
- `GeneratorSource` - Generate data for testing purposes.
- `GHarchiveSource` - Read data from Github Archive, support local file and http get ( https://data.gharchive.org/  )

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
fluxus-sources = "0.1"
```