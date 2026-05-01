mod server;

use std::net::SocketAddr;

use clap::Parser;
use server::flight;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    println!("mangrobe-db Flight server listening on {}", cli.addr);
    flight::serve(cli.addr).await?;

    Ok(())
}
