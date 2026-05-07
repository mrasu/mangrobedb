use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use super::{import, query};
use crate::app_config::AppConfig;
use crate::application::import::service::ImportService;
use crate::application::query::service::QueryService;
use crate::domain::common_ports::CommonPorts;
use crate::infrastructure::catalog::mangrobe::MangrobeCatalog;
use crate::infrastructure::object_store::S3ObjectStore;
use crate::infrastructure::uuid::RandomUuid;
use crate::util::db::connect;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, stream};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

pub type SharedImportService = Arc<ImportService<MangrobeCatalog, S3ObjectStore>>;
pub type SharedQueryService = Arc<QueryService<MangrobeCatalog, S3ObjectStore>>;

#[derive(Debug)]
pub struct MangrobeFlightService {
    import_service: SharedImportService,
    query_service: SharedQueryService,
}

impl MangrobeFlightService {
    pub fn new(import_service: SharedImportService, query_service: SharedQueryService) -> Self {
        Self {
            import_service,
            query_service,
        }
    }
}

pub async fn serve(addr: SocketAddr, app_config: &AppConfig) -> Result<(), anyhow::Error> {
    // Use MockCatalog for easy development without Mangrobe API server
    // let catalog_port = Arc::new(MockCatalog::load_default()?);
    let catalog_port = Arc::new(build_catalog(app_config).await?);

    let common_ports = Arc::new(CommonPorts::new(Arc::new(RandomUuid)));
    let object_store_port = Arc::new(S3ObjectStore::from_env(&app_config.s3.bucket)?);

    let import_service = Arc::new(ImportService::new(
        Arc::clone(&catalog_port),
        Arc::clone(&object_store_port),
        Arc::clone(&common_ports),
    ));
    let query_service = Arc::new(QueryService::new(
        Arc::clone(&catalog_port),
        Arc::clone(&object_store_port),
    ));

    Server::builder()
        .add_service(FlightServiceServer::new(MangrobeFlightService::new(
            import_service,
            query_service,
        )))
        .serve_with_shutdown(addr, async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("failed to listen for ctrl-c: {error}");
            }
        })
        .await?;

    catalog_port.save_current_state()?;

    Ok(())
}

async fn build_catalog(app_config: &AppConfig) -> Result<MangrobeCatalog, anyhow::Error> {
    let db = connect(app_config.database_url.clone()).await?;
    let catalog = MangrobeCatalog::load_default(db)?;

    Ok(catalog)
}

#[tonic::async_trait]
impl FlightService for MangrobeFlightService {
    type HandshakeStream = ResponseStream<HandshakeResponse>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake is not implemented"))
    }

    type ListFlightsStream = ResponseStream<FlightInfo>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info is not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema is not implemented"))
    }

    type DoGetStream = ResponseStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let output = query::handle_do_get(&self.query_service, request.into_inner())
            .await
            .map_err(|error| error.handle_then_to_status())?;

        Ok(Response::new(output))
    }

    type DoPutStream = ResponseStream<PutResult>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        import::handle_do_put(&self.import_service, request.into_inner())
            .await
            .map_err(|error| error.handle_then_to_status())?;

        Ok(Response::new(Box::pin(stream::empty())))
    }

    type DoExchangeStream = ResponseStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not implemented"))
    }

    type DoActionStream = ResponseStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not implemented"))
    }

    type ListActionsStream = ResponseStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions is not implemented"))
    }
}
