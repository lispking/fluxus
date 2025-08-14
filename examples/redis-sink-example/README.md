# Redis Sink Example

This example demonstrates how to use the Redis sink in Fluxus stream processing engine.

## Prerequisites

1. **Redis Server**: Make sure Redis is running on your system
   ```bash
   # Install Redis (macOS)
   brew install redis
   
   # Start Redis server
   redis-server
   
   # Or use Docker
   docker run -d -p 6379:6379 redis:latest
   ```

2. **Verify Redis Connection**:
   ```bash
   redis-cli ping
   # Should return: PONG
   ```

## Running the Example

```bash
cargo run --example redis-sink-example
```

## What This Example Does

The example demonstrates five different Redis operations:

### 1. SET Operation
- Stores sensor data as individual keys
- Key format: `sensor_data:{timestamp}`
- Value: JSON serialized sensor data

### 2. HSET Operation  
- Stores sensor data in a hash structure
- Hash name: `sensor_hash`
- Field format: `reading:{timestamp}`
- Value: JSON serialized sensor data

### 3. LPUSH Operation
- Pushes sensor data to the left side of a list
- List name: `sensor_list_left`
- Value: JSON serialized sensor data

### 4. RPUSH Operation
- Pushes sensor data to the right side of a list
- List name: `sensor_list_right`
- Value: JSON serialized sensor data

### 5. Stream Operation (XADD)
- Adds sensor data to a Redis Stream for queue processing
- Stream name: `sensor_events`
- Fields: `data` (JSON) and `timestamp` (milliseconds)
- Ideal for producer-consumer patterns

## Inspecting Results in Redis

After running the example, you can inspect the stored data:

```bash
# Connect to Redis CLI
redis-cli

# Check SET keys
KEYS sensor_data:*
GET sensor_data:1234567890

# Check HSET data
HGETALL sensor_hash

# Check LPUSH list (newest items first)
LRANGE sensor_list_left 0 -1

# Check RPUSH list (oldest items first)
LRANGE sensor_list_right 0 -1

# Check Stream entries (newest items first)
XREAD STREAMS sensor_events 0
XRANGE sensor_events - +
```

## Configuration

The Redis sink supports various connection formats:

```rust
// Basic connection
RedisSink::set("redis://127.0.0.1:6379", "my_key")

// With authentication
RedisSink::set("redis://:password@127.0.0.1:6379", "my_key")

// With database selection
RedisSink::set("redis://127.0.0.1:6379/2", "my_key")

// Stream for queue processing
RedisSink::stream("redis://127.0.0.1:6379", "events")
```
