use anyhow::Result;
use fluxus_api::{
    DataStream,
    io::{CollectionSink, CollectionSource},
};
use fluxus_core::WindowConfig;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

#[derive(Clone)]
#[allow(dead_code)]
pub struct StockTrade {
    symbol: String,
    price: f64,
    volume: u64,
    timestamp: SystemTime,
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct StockStats {
    symbol: String,
    vwap: f64, // Volume Weighted Average Price
    total_volume: u64,
    price_change: f64,
    high: f64,
    low: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Generate sample stock trading data
    let trades = generate_sample_trades();
    let source = CollectionSource::new(trades);
    let sink = CollectionSink::new();

    // Build and execute stream processing pipeline
    DataStream::new(source)
        // Group by stock symbol
        .map(|trade| (trade.symbol.clone(), trade))
        // Create 5-minute sliding window with 1-minute slide
        .window(WindowConfig::sliding(
            Duration::from_secs(300), // 5 minutes
            Duration::from_secs(60),  // 1 minute
        ))
        // Aggregate stock statistics within each window
        .aggregate(HashMap::new(), |mut stats, (symbol, trade)| {
            let entry = stats.entry(symbol.clone()).or_insert_with(|| StockStats {
                symbol,
                vwap: 0.0,
                total_volume: 0,
                price_change: 0.0,
                high: trade.price,
                low: trade.price,
            });

            // Update statistics
            let volume_price =
                (entry.vwap * entry.total_volume as f64) + (trade.price * trade.volume as f64);
            entry.total_volume += trade.volume;
            entry.vwap = volume_price / entry.total_volume as f64;
            entry.high = entry.high.max(trade.price);
            entry.low = entry.low.min(trade.price);
            entry.price_change = entry.high - entry.low;

            stats
        })
        // Output results to sink
        .sink(sink.clone())
        .await?;

    // Print results
    println!("\nStock Market Statistics:");
    for result in sink.get_data() {
        for (symbol, stats) in result {
            println!(
                "Stock: {}, VWAP: {:.2}, Volume: {}, Price Change: {:.2}, High: {:.2}, Low: {:.2}",
                symbol, stats.vwap, stats.total_volume, stats.price_change, stats.high, stats.low
            );
        }
    }

    Ok(())
}

// Generate sample trading data
fn generate_sample_trades() -> Vec<StockTrade> {
    let symbols = vec!["AAPL", "GOOGL", "MSFT", "AMZN"];
    let mut trades = Vec::new();
    let start_time = SystemTime::now();

    for i in 0..100 {
        for symbol in &symbols {
            let base_price = match *symbol {
                "AAPL" => 150.0,
                "GOOGL" => 2800.0,
                "MSFT" => 300.0,
                "AMZN" => 3300.0,
                _ => 100.0,
            };

            // Simulate price fluctuation
            let price_variation = (i as f64 * 0.1).sin() * 5.0;
            let trade = StockTrade {
                symbol: symbol.to_string(),
                price: base_price + price_variation,
                volume: 100 + (i as u64 % 900),
                timestamp: start_time + Duration::from_secs(i as u64 * 30), // Data point every 30 seconds
            };
            trades.push(trade);
        }
    }

    trades
}
