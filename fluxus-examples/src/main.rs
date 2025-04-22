use clap::{Parser, ValueEnum};
use anyhow::Result;

mod word_count;
mod temperature_sensor;
mod click_stream;
mod network_log;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.example {
        Example::WordCount => word_count::run().await,
        Example::Temperature => temperature_sensor::run().await,
        Example::ClickStream => click_stream::run().await,
        Example::NetworkLog => network_log::run().await,
    }
}
