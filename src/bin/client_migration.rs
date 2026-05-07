use sea_orm_migration::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli::run_cli(mangrobe_api_server::migration::Migrator).await;
    Ok(())
}
