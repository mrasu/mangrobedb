mod application;
mod di;
mod domain;
mod infrastructure;
mod server;
mod util;

use std::net::SocketAddr;

use clap::Parser;
use server::flight;
use tracing_subscriber::EnvFilter;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    println!("mangrobe-db Flight server listening on {}", cli.addr);
    flight::serve(cli.addr).await?;

    Ok(())
}
