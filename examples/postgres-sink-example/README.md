# PostgreSQL Sink Example

This example demonstrates how to use the PostgreSQL sink in Fluxus stream processing engine.

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

2. **Create Database and Table**:
   ```sql
   -- Connect to PostgreSQL
   psql -U postgres
   
   -- Create database
   CREATE DATABASE fluxus_test;
   
   -- Connect to the database
   \c fluxus_test;
   
   -- Create table for storing events
   CREATE TABLE user_events (
       id SERIAL PRIMARY KEY,
       data JSONB NOT NULL,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   
   -- Create index on JSONB data for better query performance
   CREATE INDEX idx_user_events_data ON user_events USING GIN (data);
   ```

## Running the Example

```bash
cargo run --example postgres-sink-example
```

## What This Example Does

The example demonstrates three PostgreSQL sink configurations:

### 1. Simple Sink (Data Only)
- Stores user event data in JSONB format
- Only inserts the data column
- Uses PostgreSQL's default timestamp for created_at

### 2. Sink with Timestamp
- Stores user event data in JSONB format  
- Includes the record timestamp from Fluxus
- Converts millisecond timestamp to PostgreSQL TIMESTAMP

### 3. Structured Sink with Column Mapping
- Maps JSON fields to specific PostgreSQL columns
- Supports type conversion (BigInt, Text, Double, etc.)
- Batch processing for better performance
- Demonstrates the full power of structured data mapping

## Inspecting Results in PostgreSQL

After running the example, you can query the stored data:

```sql
-- Connect to the database
psql -U postgres -d fluxus_test

-- View all stored events
SELECT id, data, created_at FROM user_events ORDER BY id;

-- Query specific event types
SELECT data->>'event_type' as event_type, 
       data->>'page' as page,
       created_at
FROM user_events 
WHERE data->>'event_type' = 'page_view';

-- Query events by user_id
SELECT data->>'user_id' as user_id,
       data->>'event_type' as event_type,
       data->>'page' as page
FROM user_events 
WHERE (data->>'user_id')::bigint = 12345;

-- Aggregate query - count events by type
SELECT data->>'event_type' as event_type, 
       COUNT(*) as event_count
FROM user_events 
GROUP BY data->>'event_type'
ORDER BY event_count DESC;

-- Query structured data from user_analytics table
SELECT user_id, event_type, page_path, 
       duration_seconds, browser_type, created_at
FROM user_analytics 
ORDER BY created_at DESC;

-- Analyze user behavior patterns
SELECT event_type, 
       AVG(duration_seconds) as avg_duration,
       COUNT(*) as event_count
FROM user_analytics 
GROUP BY event_type
ORDER BY avg_duration DESC;
```

## Configuration

The PostgreSQL sink supports various connection formats:

```rust
// Simple configuration (data only)
PostgresSink::simple(
    "host=localhost user=postgres dbname=mydb",
    "events_table",
    "json_data"
)

// With timestamp column
PostgresSink::with_timestamp(
    "host=localhost user=postgres dbname=mydb",
    "events_table", 
    "json_data",
    "event_timestamp"
)

// Full configuration with password
PostgresSink::simple_jsonb(
    "host=localhost user=postgres password=secret dbname=mydb",
    "events_table",
    "json_data"
)

// Structured mapping with builder pattern
PostgresSink::builder("conn_str", "analytics_table")
    .map_column("user_id", "user.id", PostgresType::BigInt, true)
    .map_column("event_name", "event_type", PostgresType::Text, true)
    .map_column("duration", "duration_ms", PostgresType::Double, false)
    .with_timestamp("created_at")
    .batch_size(100)
    .build()
```

## Schema Flexibility

The PostgreSQL sink stores data as JSONB, which provides:

- **Schema flexibility**: No need to predefine columns for your data
- **Query performance**: Native JSON operators and indexing
- **Data validation**: PostgreSQL validates JSON structure
- **Compression**: JSONB format is more compact than JSON

Example queries using JSONB operators:

```sql
-- Find events with duration > 1000ms
SELECT * FROM user_events WHERE (data->>'duration_ms')::bigint > 1000;

-- Find events containing specific text in user_agent
SELECT * FROM user_events WHERE data->>'user_agent' ILIKE '%chrome%';

-- Extract nested data
SELECT data->'metadata'->>'source' as source FROM user_events;
```
