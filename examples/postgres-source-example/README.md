# PostgreSQL Source Example

This example demonstrates how to use the PostgreSQL source in Fluxus stream processing engine to read data from a PostgreSQL database.

## Prerequisites

1. **PostgreSQL Server**: Make sure PostgreSQL is running on your system
   ```bash
   # Install PostgreSQL (macOS)
   brew install postgresql
   
   # Start PostgreSQL server
   brew services start postgresql
   
   # Or use Docker
   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:latest
   ```

2. **Create Database and Sample Data**:
   ```sql
   -- Connect to PostgreSQL
   psql -U postgres
   
   -- Create database
   CREATE DATABASE fluxus_test;
   
   -- Connect to the database
   \c fluxus_test;
   
   -- Create table for log entries
   CREATE TABLE log_entries (
       id SERIAL PRIMARY KEY,
       level TEXT NOT NULL,
       message TEXT NOT NULL,
       service TEXT NOT NULL,
       timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   
   -- Insert sample data
   INSERT INTO log_entries (level, message, service) VALUES
       ('INFO', 'Application started', 'web-server'),
       ('ERROR', 'Database connection failed', 'api-service'),
       ('WARN', 'High memory usage detected', 'worker'),
       ('INFO', 'User logged in', 'auth-service'),
       ('DEBUG', 'Cache miss for key user:123', 'cache-service'),
       ('ERROR', 'Payment processing failed', 'payment-service');
   ```

## Running the Example

```bash
cargo run --example postgres-source-example
```

## What This Example Does

The example demonstrates two PostgreSQL source configurations:

### 1. Single Query Source
- Executes a SQL query once and returns all results
- Perfect for batch processing or data migration
- Automatically handles type conversion from PostgreSQL to Rust structs

### 2. Polling Source (Commented)
- Periodically polls the database for new data
- Ideal for real-time data processing
- Can track the last processed record to avoid duplicates

## Use Cases

### Data Migration
```rust
// Read from source database
let source = PostgresSource::query(
    "host=source-db user=postgres dbname=old_system",
    "SELECT * FROM legacy_events ORDER BY created_at"
);

// Process and write to target database
// (combine with PostgreSQL sink for complete migration)
```

### Real-time Analytics
```rust
// Poll for new log entries every 30 seconds
let source = PostgresSource::polling(
    "host=analytics-db user=postgres dbname=logs",
    "SELECT * FROM access_logs WHERE processed_at IS NULL",
    Duration::from_secs(30)
);
```

### Data Replay for Testing
```rust
// Replay historical events in chronological order
let source = PostgresSource::query(
    "host=test-db user=postgres dbname=test_data",
    "SELECT * FROM historical_events WHERE date >= '2024-01-01' ORDER BY timestamp"
);
```

## Configuration

The PostgreSQL source supports various connection formats:

```rust
// Basic connection
PostgresSource::query("host=localhost user=postgres dbname=mydb", "SELECT * FROM table")

// With password
PostgresSource::query("host=localhost user=postgres password=secret dbname=mydb", "SELECT * FROM table")

// With parameters
PostgresSource::query_with_params(
    "host=localhost user=postgres dbname=mydb",
    "SELECT * FROM events WHERE user_id = $1 AND date > $2",
    vec!["123", "2024-01-01"]
)

// Polling with interval
PostgresSource::polling(
    "host=localhost user=postgres dbname=mydb",
    "SELECT * FROM new_events WHERE id > $1",
    Duration::from_secs(10)
)
```

## Data Type Mapping

The PostgreSQL source automatically maps PostgreSQL types to JSON:

- `TEXT`, `VARCHAR` → `String`
- `INT4` → `i32`
- `INT8`, `BIGINT` → `i64`
- `FLOAT8`, `DOUBLE PRECISION` → `f64`
- `BOOL` → `bool`
- `JSON`, `JSONB` → `serde_json::Value`
- Other types → `String` (fallback)

## Schema Flexibility

### Single Column (JSON)
If your query returns a single column containing JSON data:
```sql
SELECT data FROM events;  -- where 'data' is JSONB
```
The source will directly deserialize the JSON to your Rust type.

### Multiple Columns
If your query returns multiple columns:
```sql
SELECT id, name, email, created_at FROM users;
```
The source will create a JSON object with column names as keys:
```json
{
  "id": 123,
  "name": "John Doe", 
  "email": "john@example.com",
  "created_at": "2024-01-01T10:00:00"
}
```

This JSON is then deserialized to your Rust struct.
