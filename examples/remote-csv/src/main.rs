use fluxus_sources::{CsvSource, Source};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv";

    println!("Reading CSV data from: {}", url);

    let mut source = CsvSource::from_url(url);

    source.init().await?;

    for i in 0..10 {
        match source.next().await? {
            Some(record) => println!("Line {}: {}", i + 1, record.data),
            None => {
                println!("End of file reached");
                break;
            }
        }
    }

    source.close().await?;

    println!("Done!");

    Ok(())
}
