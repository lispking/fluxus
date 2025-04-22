use fluxus_api::{DataStream, io::{CollectionSource, CollectionSink}};
use fluxus_core::WindowConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::time::{SystemTime, Duration};

#[derive(Clone)]
pub struct ClickEvent {
    user_id: String,
    page_id: String,
    event_type: String,
    timestamp: SystemTime,
}

#[derive(Clone)]
pub struct UserSession {
    user_id: String,
    page_views: Vec<String>,
    start_time: SystemTime,
    duration_secs: u64,
    total_events: usize,
}

pub async fn run() -> Result<()> {
    // Generate sample click events
    let events = generate_sample_clicks();
    let source = CollectionSource::new(events);
    let sink = CollectionSink::new();

    // Build and execute the streaming pipeline
    DataStream::new(source)
        // Filter only page view events
        .filter(|event| event.event_type == "page_view")
        // Group by user_id
        .map(|event| (event.user_id.clone(), (event.page_id.clone(), event.timestamp)))
        // Create session windows with 30-second timeout
        .window(WindowConfig::Session { timeout_ms: 30000 })
        // Aggregate user sessions
        .aggregate(
            HashMap::new(),
            |mut sessions, (user_id, (page_id, timestamp))| {
                let session = sessions.entry(user_id.clone()).or_insert_with(|| UserSession {
                    user_id,
                    page_views: Vec::new(),
                    start_time: timestamp,
                    duration_secs: 0,
                    total_events: 0,
                });
                
                session.page_views.push(page_id);
                session.duration_secs = timestamp
                    .duration_since(session.start_time)
                    .unwrap_or(Duration::from_secs(0))
                    .as_secs();
                session.total_events += 1;
                
                sessions
            },
        )
        .sink(sink.clone())
        .await?;

    // Print results
    println!("\nClick stream analysis results:");
    for session_data in sink.get_data() {
        println!("\nSession window results:");
        for (_, session) in session_data {
            println!(
                "User {}: {} events over {}s, Pages: {}",
                session.user_id,
                session.total_events,
                session.duration_secs,
                session.page_views.join(" -> ")
            );
        }
    }

    Ok(())
}

// Helper function to generate sample data
fn generate_sample_clicks() -> Vec<ClickEvent> {
    let start_time = SystemTime::now();
    let mut events = Vec::new();
    let pages = vec!["home", "products", "cart", "checkout"];
    let users = vec!["user1", "user2", "user3"];
    
    for (user_idx, user_id) in users.iter().enumerate() {
        let user_start = start_time + Duration::from_secs(user_idx as u64 * 5);
        
        // Simulate a user session with page views and some other events
        for (i, &page) in pages.iter().enumerate() {
            // Add page view
            events.push(ClickEvent {
                user_id: user_id.to_string(),
                page_id: page.to_string(),
                event_type: "page_view".to_string(),
                timestamp: user_start + Duration::from_secs(i as u64 * 10),
            });
            
            // Add some click events
            events.push(ClickEvent {
                user_id: user_id.to_string(),
                page_id: page.to_string(),
                event_type: "click".to_string(),
                timestamp: user_start + Duration::from_secs(i as u64 * 10 + 2),
            });
        }
    }

    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_click_stream_processing() -> Result<()> {
        let events = vec![
            ClickEvent {
                user_id: "test_user".to_string(),
                page_id: "page1".to_string(),
                event_type: "page_view".to_string(),
                timestamp: SystemTime::now(),
            },
            ClickEvent {
                user_id: "test_user".to_string(),
                page_id: "page2".to_string(),
                event_type: "page_view".to_string(),
                timestamp: SystemTime::now() + Duration::from_secs(10),
            },
        ];

        let source = CollectionSource::new(events);
        let sink: CollectionSink<HashMap<String, UserSession>> = CollectionSink::new();

        DataStream::new(source)
            .filter(|event| event.event_type == "page_view")
            .map(|event| (event.user_id.clone(), (event.page_id.clone(), event.timestamp)))
            .window(WindowConfig::Session { timeout_ms: 30000 })
            .aggregate(
                HashMap::new(),
                |mut sessions, (user_id, (page_id, timestamp))| {
                    let session = sessions.entry(user_id.clone()).or_insert_with(|| UserSession {
                        user_id,
                        page_views: Vec::new(),
                        start_time: timestamp,
                        duration_secs: 0,
                        total_events: 0,
                    });
                    
                    session.page_views.push(page_id);
                    session.duration_secs = timestamp
                        .duration_since(session.start_time)
                        .unwrap_or(Duration::from_secs(0))
                        .as_secs();
                    session.total_events += 1;
                    
                    sessions
                },
            )
            .sink(sink.clone())
            .await?;

        let results = sink.get_data();
        assert!(!results.is_empty());
        
        if let Some(session_data) = results.first() {
            let session = session_data.get("test_user").unwrap();
            assert_eq!(session.total_events, 2);
            assert_eq!(session.page_views.len(), 2);
            assert_eq!(session.page_views[0], "page1");
            assert_eq!(session.page_views[1], "page2");
        }

        Ok(())
    }
}