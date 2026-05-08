use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use arrow_flight::sql::client::FlightSqlServiceClient;
use clap::Parser;
use futures::StreamExt;
use tonic::transport::Channel;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();
    let endpoint = format!("http://{}", config.addr);
    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut client = FlightSqlServiceClient::new(channel);

    let query_ticket = client.execute(config.sql, None).await?;
    let endpoint = query_ticket
        .endpoint
        .first()
        .ok_or_else(|| "query returned no flight endpoints".to_string())?;
    let ticket = endpoint
        .ticket
        .as_ref()
        .ok_or_else(|| "query endpoint did not include a ticket".to_string())?;
    println!("ticket: {:?}", ticket);

    let mut ret = client.do_get(ticket.clone()).await?;

    let mut query_result = Vec::<RecordBatch>::new();
    while let Some(next) = ret.next().await {
        let batch = next?;

        query_result.push(batch);
    }
    print_batches(&query_result)?;

    Ok(())
}

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: String,

    #[arg(long)]
    sql: String,
}
