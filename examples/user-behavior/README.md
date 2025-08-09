# User Behavior Analysis Example

This example demonstrates how to use Fluxus for analyzing user behavior patterns in real-time web analytics scenarios.

## Overview

The user behavior analysis example showcases:

- **Session-based Analysis**: Groups user events by session ID to analyze complete user journeys
- **Multi-metric Tracking**: Calculates various engagement metrics including page views, clicks, scrolls, and session duration
- **Bounce Rate Calculation**: Determines user engagement by analyzing single-page sessions
- **Device-based Segmentation**: Tracks user behavior across different device types (desktop, mobile, tablet)
- **Real-time Processing**: Uses Fluxus streaming capabilities for live user behavior analysis

## Features

### Event Types Processed
- **Page Views**: Track which pages users visit and in what order
- **Clicks**: Monitor user interactions and engagement
- **Scrolls**: Measure content consumption patterns

### Metrics Calculated
- Total events per session
- Page views, clicks, and scroll counts
- Session duration
- Unique pages visited
- Bounce rate (single-page sessions)
- Most visited page per session
- Complete user journey mapping

### Data Structures

#### UserEvent
```rust
pub struct UserEvent {
    user_id: String,
    event_type: String,      // "page_view", "click", "scroll"
    page: String,            // Page URL or identifier
    timestamp: SystemTime,
    session_id: String,      // Groups events into sessions
    device_type: String,     // "desktop", "mobile", "tablet"
    duration_ms: Option<u64>, // Time spent on action
}
```

#### UserBehaviorMetrics
```rust
pub struct UserBehaviorMetrics {
    user_id: String,
    session_id: String,
    total_events: usize,
    page_views: usize,
    clicks: usize,
    scrolls: usize,
    session_duration_ms: u64,
    unique_pages: usize,
    bounce_rate: f64,
    device_type: String,
    pages_visited: Vec<String>,
    most_visited_page: String,
}
```

## How It Works

1. **Event Generation**: Creates sample user events across multiple users, sessions, and device types
2. **Session Grouping**: Groups events by session ID using session windows with 5-minute timeout
3. **Metrics Aggregation**: Calculates comprehensive behavior metrics for each session
4. **Real-time Analysis**: Processes events as they arrive and updates metrics continuously

## Running the Example

```bash
# From the project root
cargo run --example user-behavior

# Or from the example directory
cd examples/user-behavior
cargo run
```

## Sample Output

```
🚀 Starting Fluxus User Behavior Analysis Example
📊 Generated 50 sample user events

📈 User Behavior Analysis Results:
=====================================

🔍 Window 1 Analysis:

👤 User: alice (Session: session_alice_0)
  📱 Device: desktop
  📊 Total Events: 8
  👁️  Page Views: 3
  🖱️  Clicks: 3
  📜 Scrolls: 2
  ⏱️  Session Duration: 12.5s
  🌐 Unique Pages: 3
  📈 Bounce Rate: 0.0%
  🏆 Most Visited Page: /home
  🗺️  Pages Journey: /home → /products → /cart

✅ User behavior analysis completed successfully!
💡 This example demonstrates:
   - Session-based user behavior tracking
   - Real-time metrics calculation
   - Multi-device user journey analysis
   - Bounce rate and engagement metrics
```

## Use Cases

This pattern is useful for:

- **Web Analytics**: Track user behavior on websites and web applications
- **E-commerce Analysis**: Understand customer journey and conversion patterns
- **Mobile App Analytics**: Monitor user engagement in mobile applications
- **A/B Testing**: Compare user behavior across different variants
- **Real-time Personalization**: Adapt content based on current user behavior
- **Fraud Detection**: Identify unusual user behavior patterns

## Extending the Example

You can extend this example by:

- Adding more event types (form submissions, video plays, downloads)
- Implementing funnel analysis for conversion tracking
- Adding geographic or demographic segmentation
- Integrating with external analytics services
- Adding machine learning for behavior prediction
- Implementing real-time alerting for unusual patterns

## Related Examples

- **click-stream**: Basic click tracking and session analysis
- **network-log**: HTTP request analysis and monitoring
- **iot-devices**: Multi-device data processing patterns