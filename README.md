# Fluxus Stream Processing Engine

Fluxus is a lightweight stream processing engine written in Rust, designed for efficient real-time data processing and analysis.

## Features

- High-performance stream processing
- Flexible windowing operations (Tumbling, Sliding, Session windows)
- Parallel processing support
- Rich set of stream operations (map, filter, aggregate)
- Type-safe API
- Easy to use and extend

## Project Structure

- `fluxus-api` - Core API definitions and interfaces
- `fluxus-core` - Core implementations and data structures
- `fluxus-runtime` - Runtime engine and execution environment
- `fluxus-examples` - Example applications demonstrating usage

## Examples

The project includes several example applications that demonstrate different use cases:

### Word Count
Simple word frequency analysis in text streams using tumbling windows.
```bash
cargo run --bin fluxus-examples -- word-count
```

### Temperature Sensor Analysis
Processing and analyzing temperature sensor data with sliding windows.
```bash
cargo run --bin fluxus-examples -- temperature
```

### Click Stream Analysis
Analyzing user click streams with session windows.
```bash
cargo run --bin fluxus-examples -- click-stream
```

### Network Log Analysis
Processing network logs with sliding windows and aggregations.
```bash
cargo run --bin fluxus-examples -- network-log
```

### View Available Examples
To see all available examples and options:
```bash
cargo run --bin fluxus-examples -- --help
```

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
cargo run --bin fluxus-examples -- [example-name]
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