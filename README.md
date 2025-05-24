<p align="center">
  <img src="docs/images/fluxus-logo.png" height="200" alt="Fluxus Logo">
</p>

# Fluxus Stream Processing Engine

[![Crates.io](https://img.shields.io/crates/v/fluxus-core.svg)](https://crates.io/crates/fluxus-core)
[![Documentation](https://docs.rs/fluxus-core/badge.svg)](https://docs.rs/fluxus-core)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache2.0-yellow.svg)](https://opensource.org/license/apache-2-0)
[<img alt="build status" src="https://img.shields.io/github/actions/workflow/status/lispking/fluxus/ci.yml?branch=main&style=for-the-badge" height="20">](https://github.com/lispking/fluxus/actions?query=branch%3Amain)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/lispking/fluxus)


Fluxus is a lightweight stream processing engine written in Rust, designed for efficient real-time data processing and analysis.

![Fluxus Architecture](docs/architecture.png)

## Features

- High-performance stream processing
- Flexible windowing operations (Tumbling, Sliding, Session windows)
- Parallel processing support
- Rich set of stream operations (map, filter, aggregate)
- Type-safe API
- Easy to use and extend

## Project Structure

- `crates/fluxus` - Main crate containing the Fluxus engine and its dependencies
- `crates/fluxus-api` - Core API definitions and interfaces
- `crates/fluxus-core` - Core implementations and data structures
- `crates/fluxus-runtime` - Runtime engine and execution environment
- `crates/fluxus-sinks` - Sink implementations for different data sinks (e.g., Kafka, Console)
- `crates/fluxus-sources` - Source implementations for different data sources (e.g., Kafka, Console)
- `crates/fluxus-transforms` - Transformations for stream processing (e.g., map, filter, aggregate)
- `crates/fluxus-utils` - Utility functions and helpers
- `examples` - Example applications demonstrating usage

## Examples

The project includes several example applications that demonstrate different use cases:

### Word Count

Simple word frequency analysis in text streams using tumbling windows.

```bash
cargo run --example word-count
```

### Temperature Sensor Analysis

Processing and analyzing temperature sensor data with sliding windows.

```bash
cargo run --example temperature-sensor
```

### Click Stream Analysis

Analyzing user click streams with session windows.

```bash
cargo run --example click-stream
```

### Network Log Analysis

Processing network logs with sliding windows and aggregations.

```bash
cargo run --example network-log
```

### Remote CSV Data Processing

Processing CSV data from remote sources like GitHub.

```bash
cargo run --example remote-csv
```

### View Available Examples

To see all available examples and options:

```bash
cargo run --example
```

## Using Fluxus in Your Project

To use Fluxus in your project, add it as a dependency using cargo:

```bash
cargo add fluxus --features full
```

This will add Fluxus with all available features to your project. After adding the dependency, you can start using Fluxus in your code. Check out the examples section below for usage examples.

## Getting Started

1. Clone the repository:

```bash
git clone https://github.com/lispking/fluxus.git
cd fluxus
```

2. Build the project:

```bash
cargo build
```

3. Run the examples:

```bash
cargo run --example [example-name]
```

## Development

### Prerequisites

- Rust 1.75+ 
- Cargo 

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=lispking/fluxus&type=Date)](https://www.star-history.com/#lispking/fluxus&Date)

### Thank you for your support and participation ❤️

<div align="center">
  <a href="https://github.com/lispking/fluxus/graphs/contributors">
    <img src="https://contrib.rocks/image?repo=lispking/fluxus" width="100%" />
  </a>
</div>
