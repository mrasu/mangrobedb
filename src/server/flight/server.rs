use crate::app_config::AppConfig;
use crate::application::flusher::service::FlushService;
use crate::application::import::service::ImportService;
use crate::application::query::service::QueryService;
use crate::domain::common_ports::CommonPorts;
use crate::infrastructure::catalog::mangrobe::MangrobeCatalog;
use crate::infrastructure::object_store::S3ObjectStore;
use crate::infrastructure::uuid::RandomUuid;
use crate::server::flight::sql_service::SqlService;
use crate::server::task::flusher::Flusher;
use crate::util::db::connect;
use arrow_flight::FlightData;
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::Stream;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tonic::Status;
use tonic::transport::Server;
use tracing::error;

pub type SharedImportService = Arc<ImportService<MangrobeCatalog, S3ObjectStore>>;
pub type SharedQueryService = Arc<QueryService<MangrobeCatalog, S3ObjectStore>>;
pub type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

pub async fn serve(addr: SocketAddr, app_config: &AppConfig) -> Result<(), anyhow::Error> {
    let catalog_port = Arc::new(build_catalog(app_config).await?);

    let common_ports = Arc::new(CommonPorts::new(Arc::new(RandomUuid)));
    let object_store_port = Arc::new(S3ObjectStore::from_env(&app_config.s3.bucket)?);
    let flush_service = Arc::new(FlushService::new(
        Arc::clone(&catalog_port),
        Arc::clone(&object_store_port),
        Arc::clone(&common_ports),
        app_config.flush_interval,
    ));

    let import_service = Arc::new(ImportService::new(
        Arc::clone(&catalog_port),
        Arc::clone(&object_store_port),
        Arc::clone(&flush_service),
    ));
    let query_service = Arc::new(QueryService::new(
        Arc::clone(&catalog_port),
        Arc::clone(&object_store_port),
    ));
    let flusher_handle = Flusher::new(Arc::clone(&flush_service)).spawn();

    Server::builder()
        .add_service(FlightServiceServer::new(SqlService::new(
            import_service,
            query_service,
        )))
        .serve_with_shutdown(addr, async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("failed to listen for ctrl-c: {error}");
            }
        })
        .await?;

    if let Err(error) = flusher_handle.shutdown().await {
        error!("failed to shutdown flusher: {error}");
    }

    Ok(())
}

async fn build_catalog(app_config: &AppConfig) -> Result<MangrobeCatalog, anyhow::Error> {
    let db = connect(app_config.database_url.clone()).await?;
    let catalog = MangrobeCatalog::new(db);

    Ok(catalog)
}
