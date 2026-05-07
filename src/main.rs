mod app_config;
mod application;
mod domain;
mod infrastructure;
mod server;
mod util;

use crate::app_config::AppConfig;
use clap::Parser;
use server::flight;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: SocketAddr,
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let app_config = AppConfig::load(cli.config.as_deref())?;

    println!("mangrobe-db Flight server listening on {}", cli.addr);
    flight::serve(cli.addr, &app_config).await?;

    Ok(())
}
