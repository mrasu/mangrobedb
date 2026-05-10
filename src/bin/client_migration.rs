#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    mangrobe_api_server::migration::run_cli().await;
    Ok(())
}
