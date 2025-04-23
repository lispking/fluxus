use anyhow::Result;
use clap::{Parser, ValueEnum};

mod click_stream;
mod iot_devices;
mod log_anomaly;
mod network_log;
mod stock_market;
mod temperature_sensor;
mod word_count;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Which example to run
    #[arg(value_enum)]
    example: Example,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Example {
    /// Word count streaming example
    WordCount,
    /// Temperature sensor data analysis
    Temperature,
    /// Click stream analysis
    ClickStream,
    /// Network log analysis
    NetworkLog,
    /// Stock market data analysis
    StockMarket,
    /// IoT devices data processing
    IoTDevices,
    /// Log anomaly detection
    LogAnomaly,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.example {
        Example::WordCount => word_count::run().await,
        Example::Temperature => temperature_sensor::run().await,
        Example::ClickStream => click_stream::run().await,
        Example::NetworkLog => network_log::run().await,
        Example::StockMarket => stock_market::run().await,
        Example::IoTDevices => iot_devices::run().await,
        Example::LogAnomaly => log_anomaly::run().await,
    }
}
