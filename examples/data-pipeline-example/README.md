# Complete Data Pipeline Example

This example demonstrates a real-world data processing pipeline using Fluxus stream processing engine:

**PostgreSQL (Raw Data) → Redis (Queue) → PostgreSQL (Analytics)**

This pattern is commonly used for:
- **Decoupling** data ingestion from processing
- **Buffering** high-volume data streams
- **Scaling** processing independently
- **Reliability** through persistent queues

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │      Redis      │    │   PostgreSQL    │
│   (Raw Data)    │───▶│     (Queue)     │───▶│   (Analytics)   │
│                 │    │                 │    │                 │
│ • raw_events    │    │ • Stream        │    │ • user_analytics│
│ • Unprocessed   │    │ • Buffering     │    │ • Structured    │
│ • High volume   │    │ • Decoupling    │    │ • Optimized     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Prerequisites

### 1. PostgreSQL Setup
```bash
# Install and start PostgreSQL
brew install postgresql
brew services start postgresql

# Or use Docker
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:latest
```

### 2. Redis Setup
```bash
# Install and start Redis
brew install redis
brew services start redis

# Or use Docker
docker run -d -p 6379:6379 redis:latest
```

### 3. Database Schema
```sql
-- Connect to PostgreSQL
psql -U postgres

-- Create database
CREATE DATABASE fluxus_test;
\c fluxus_test;

-- Create raw events table (source)
CREATE TABLE raw_events (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    page TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Create analytics table (destination)
CREATE TABLE user_analytics (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    page_path TEXT,
    event_timestamp TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO raw_events (user_id, event_type, page, metadata) VALUES
    (12345, 'page_view', '/home', '{"referrer": "google.com", "device": "desktop"}'),
    (67890, 'button_click', '/products', '{"button_id": "add_to_cart", "product_id": 123}'),
    (12345, 'form_submit', '/contact', '{"form_type": "contact", "fields": 5}'),
    (11111, 'page_view', '/about', '{"session_id": "abc123", "duration": 45}'),
    (67890, 'purchase', '/checkout', '{"amount": 99.99, "currency": "USD"}');
```

## Running the Example

```bash
cargo run --example data-pipeline-example
```

## What This Example Does

### Stage 1: Producer (PostgreSQL → Redis)
1. **Reads** raw events from PostgreSQL `raw_events` table
2. **Processes** each event (transformation logic can be added here)
3. **Queues** events to Redis Stream for reliable buffering

### Stage 2: Consumer (Redis → PostgreSQL)
1. **Consumes** events from Redis Stream queue
2. **Transforms** data into structured format
3. **Batches** writes for better performance
4. **Stores** in PostgreSQL `user_analytics` table with proper typing

## Key Benefits Demonstrated

### 1. **Decoupling**
- Producer and consumer run independently
- Can scale each stage separately
- Failure in one stage doesn't affect the other

### 2. **Reliability**
- Redis Stream provides persistent queuing
- Data won't be lost if consumer is temporarily down
- Can replay events from any point in time

### 3. **Performance**
- Batch processing in the analytics sink
- Asynchronous processing pipeline
- Efficient resource utilization

### 4. **Flexibility**
- Easy to add transformation logic between stages
- Can add multiple consumers for different analytics
- Simple to add monitoring and alerting

## Real-World Use Cases

### 1. **E-commerce Analytics**
```
User Actions → Queue → Analytics Dashboard
• Page views, clicks, purchases
• Real-time metrics and reporting
• A/B testing data collection
```

### 2. **Log Processing**
```
Application Logs → Queue → Search/Analytics
• Error tracking and alerting
• Performance monitoring
• Security event detection
```

### 3. **IoT Data Pipeline**
```
Sensor Data → Queue → Time Series DB
• Temperature, humidity, pressure
• Real-time monitoring dashboards
• Predictive maintenance alerts
```

### 4. **Financial Transactions**
```
Transaction Events → Queue → Risk Analysis
• Fraud detection
• Compliance reporting
• Real-time notifications
```

## Extending the Example

### Add Data Transformation
```rust
// In producer_pipeline(), before queuing:
let mut processed_event = record.data.clone();
processed_event.metadata = enhance_metadata(processed_event.metadata);
```

### Add Multiple Consumers
```rust
// Create specialized consumers for different purposes
let metrics_consumer = create_metrics_consumer();
let alerts_consumer = create_alerts_consumer();
```

### Add Error Handling
```rust
// Implement retry logic and dead letter queues
match redis_sink.write(record).await {
    Ok(_) => info!("Event processed successfully"),
    Err(e) => {
        warn!("Processing failed: {}", e);
        // Send to dead letter queue for manual review
        dead_letter_sink.write(record).await?;
    }
}
```

## Monitoring and Observability

The example includes extensive logging to show:
- **Data flow** through each stage
- **Performance metrics** (events processed)
- **Error handling** and recovery
- **Resource utilization**

In production, you would add:
- Metrics collection (Prometheus)
- Distributed tracing (Jaeger)
- Health checks and alerting
- Dashboard visualization (Grafana)

This example demonstrates how Fluxus can be used to build robust, scalable data processing pipelines for real-world applications.
