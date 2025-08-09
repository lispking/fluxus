use anyhow::Result;
use fluxus::api::{
    DataStream,
    io::{CollectionSink, CollectionSource},
};
use fluxus::utils::window::WindowConfig;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug)]
pub struct UserEvent {
    user_id: String,
    event_type: String,
    page: String,
    timestamp: SystemTime,
    session_id: String,
    device_type: String,
    duration_ms: Option<u64>,
}

#[derive(Clone, Debug)]
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🚀 Starting Fluxus User Behavior Analysis Example");

    // Generate sample user events
    let events = generate_sample_user_events();
    println!("📊 Generated {} sample user events", events.len());

    let source = CollectionSource::new(events);
    let sink = CollectionSink::new();

    // Build and execute the streaming pipeline
    DataStream::new(source)
        // Group events by session_id for behavior analysis
        .map(|event| (event.session_id.clone(), event))
        // Create session windows with 5-minute timeout
        .window(WindowConfig::session(Duration::from_secs(300)))
        // Aggregate user behavior metrics per session
        .aggregate(HashMap::new(), |mut sessions, (session_id, event)| {
            let metrics =
                sessions
                    .entry(session_id.clone())
                    .or_insert_with(|| UserBehaviorMetrics {
                        user_id: event.user_id.clone(),
                        session_id: session_id.clone(),
                        total_events: 0,
                        page_views: 0,
                        clicks: 0,
                        scrolls: 0,
                        session_duration_ms: 0,
                        unique_pages: 0,
                        bounce_rate: 0.0,
                        device_type: event.device_type.clone(),
                        pages_visited: Vec::new(),
                        most_visited_page: String::new(),
                    });

            // Update metrics
            metrics.total_events += 1;

            match event.event_type.as_str() {
                "page_view" => {
                    metrics.page_views += 1;
                    if !metrics.pages_visited.contains(&event.page) {
                        metrics.pages_visited.push(event.page.clone());
                    }
                }
                "click" => metrics.clicks += 1,
                "scroll" => metrics.scrolls += 1,
                _ => {}
            }

            // Calculate session duration and other metrics
            if let Some(duration) = event.duration_ms {
                metrics.session_duration_ms += duration;
            }

            // Use timestamp for logging (this eliminates the unused field warning)
            if metrics.total_events == 1 {
                println!("  First event timestamp: {:?}", event.timestamp);
            }

            metrics.unique_pages = metrics.pages_visited.len();
            metrics.bounce_rate = if metrics.unique_pages <= 1 { 1.0 } else { 0.0 };

            // Find most visited page
            let mut page_counts: HashMap<String, usize> = HashMap::new();
            for page in &metrics.pages_visited {
                *page_counts.entry(page.clone()).or_insert(0) += 1;
            }

            if let Some((most_visited, _)) = page_counts.iter().max_by_key(|(_, count)| *count) {
                metrics.most_visited_page = most_visited.clone();
            }

            sessions
        })
        .sink(sink.clone())
        .await?;

    // Display results
    println!("\n📈 User Behavior Analysis Results:");
    println!("=====================================");

    for (window_idx, session_data) in sink.get_data().iter().enumerate() {
        println!("\n🔍 Window {} Analysis:", window_idx + 1);

        for metrics in session_data.values() {
            println!(
                "\n👤 User: {} (Session: {})",
                metrics.user_id, metrics.session_id
            );
            println!("  📱 Device: {}", metrics.device_type);
            println!("  📊 Total Events: {}", metrics.total_events);
            println!("  👁️  Page Views: {}", metrics.page_views);
            println!("  🖱️  Clicks: {}", metrics.clicks);
            println!("  📜 Scrolls: {}", metrics.scrolls);
            println!(
                "  ⏱️  Session Duration: {:.2}s",
                metrics.session_duration_ms as f64 / 1000.0
            );
            println!("  🌐 Unique Pages: {}", metrics.unique_pages);
            println!("  📈 Bounce Rate: {:.1}%", metrics.bounce_rate * 100.0);
            println!("  🏆 Most Visited Page: {}", metrics.most_visited_page);
            println!("  🗺️  Pages Journey: {}", metrics.pages_visited.join(" → "));
        }
    }
    Ok(())
}

// Helper function to generate sample user behavior data
fn generate_sample_user_events() -> Vec<UserEvent> {
    let start_time = SystemTime::now();
    let mut events = Vec::new();

    let users = ["alice", "bob", "charlie", "diana", "eve"];
    let pages = [
        "/home",
        "/products",
        "/product/123",
        "/cart",
        "/checkout",
        "/profile",
        "/about",
    ];
    let devices = ["desktop", "mobile", "tablet"];
    let event_types = ["page_view", "click", "scroll"];

    // Generate events for multiple users and sessions
    for (user_idx, &user_id) in users.iter().enumerate() {
        // Each user has 1-2 sessions
        for session_num in 0..=1 {
            let session_id = format!("session_{user_id}_{session_num}");
            let device_type = devices[user_idx % devices.len()];
            let session_start =
                start_time + Duration::from_secs(user_idx as u64 * 60 + session_num * 300);

            // Generate 5-15 events per session
            let event_count = 5 + (user_idx * 2) + (session_num as usize * 3);

            for event_idx in 0..event_count {
                let event_time = session_start + Duration::from_secs(event_idx as u64 * 10);
                let page = pages[event_idx % pages.len()];
                let event_type = if event_idx % 3 == 0 {
                    "page_view"
                } else {
                    event_types[(event_idx + 1) % event_types.len()]
                };

                // Simulate different durations for different event types
                let duration_ms = match event_type {
                    "page_view" => Some(2000 + (event_idx as u64 * 500) % 8000), // 2-10 seconds
                    "click" => Some(100 + (event_idx as u64 * 50) % 400),        // 100-500ms
                    "scroll" => Some(50 + (event_idx as u64 * 25) % 200),        // 50-250ms
                    _ => None,
                };

                events.push(UserEvent {
                    user_id: user_id.to_string(),
                    event_type: event_type.to_string(),
                    page: page.to_string(),
                    timestamp: event_time,
                    session_id: session_id.clone(),
                    device_type: device_type.to_string(),
                    duration_ms,
                });
            }
        }
    }

    events
}
