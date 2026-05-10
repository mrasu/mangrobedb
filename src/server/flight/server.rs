use super::{import, query};
use crate::app_config::AppConfig;
use crate::application::flusher::service::FlushService;
use crate::application::import::service::ImportService;
use crate::application::query::service::QueryService;
use crate::domain::common_ports::CommonPorts;
use crate::infrastructure::catalog::mangrobe::MangrobeCatalog;
use crate::infrastructure::object_store::S3ObjectStore;
use crate::infrastructure::uuid::RandomUuid;
use crate::server::flight::error::FlightServerError;
use crate::server::task::flusher::Flusher;
use crate::util::db::connect;
use anyhow::anyhow;
use arrow::datatypes::Schema;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    CommandGetTables, CommandStatementIngest, CommandStatementQuery, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::stream;
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::error;

pub type SharedImportService = Arc<ImportService<MangrobeCatalog, S3ObjectStore>>;
pub type SharedQueryService = Arc<QueryService<MangrobeCatalog, S3ObjectStore>>;

const DEFAULT_CATALOG_NAME: &str = "mangrobe_db";
const DEFAULT_SCHEMA_NAME: &str = "default";

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

    if let Err(error) = flusher_handle.shutdown().await {
        error!("failed to shutdown flusher: {error}");
    }
    catalog_port.save_current_state()?;

    Ok(())
}

async fn build_catalog(app_config: &AppConfig) -> Result<MangrobeCatalog, anyhow::Error> {
    let db = connect(app_config.database_url.clone()).await?;
    let catalog = MangrobeCatalog::load_default(db)?;

    Ok(catalog)
}

#[tonic::async_trait]
impl FlightSqlService for MangrobeFlightService {
    type FlightService = Self;

    /// Get a FlightInfo for executing a SQL query.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("get_flight_info_statement invoked. query: {}", query.query);

        let ticket = Ticket {
            ticket: self
                .new_ticket_statement_query(&query.query)
                .as_any()
                .encode_to_vec()
                .into(),
        };

        Ok(Response::new(FlightInfo {
            endpoint: vec![FlightEndpoint {
                ticket: Some(ticket),
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ..Default::default()
        }))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|error| Status::internal(format!("unable to encode schema: {error}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    /// Get a FlightDataStream containing the query results.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = self.get_statement(&ticket)?;
        println!("do_get_statement invoked. sql: {sql}");

        let output = query::handle_do_get_statement(&self.query_service, &sql)
            .await
            .map_err(|err| err.handle_then_to_status())?;

        Ok(Response::new(Box::pin(output)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if query.include_schema {
            return Err(Status::unimplemented(
                "CommandGetTables include_schema is not supported",
            ));
        }

        let tables = self
            .query_service
            .list_tables()
            .await
            .map_err(|error| FlightServerError::from(error).handle_then_to_status())?;

        let table_schema = Schema::empty();
        let mut builder = query.into_builder();
        for table in tables {
            builder
                .append(
                    DEFAULT_CATALOG_NAME,
                    DEFAULT_SCHEMA_NAME,
                    table.table_name,
                    "TABLE",
                    &table_schema,
                )
                .map_err(Status::from)?;
        }

        let schema = builder.schema();
        let batch = builder.build().map_err(Status::from)?;
        let flight_data = batches_to_flight_data(schema.as_ref(), vec![batch])
            .map_err(|error| Status::internal(anyhow!(error).to_string()))?;
        let output = stream::iter(flight_data.into_iter().map(Ok));

        Ok(Response::new(Box::pin(output)))
    }

    /// Execute a bulk ingestion.
    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        if ticket.table_definition_options.is_some() {
            return Err(Status::invalid_argument(
                "table_definition_options like if_not_exist is not supported",
            ));
        }

        println!("do_put_statement_ingest invoked. table: {}", ticket.table);

        let num_ingested = import::do_put_statement_ingest(
            &self.import_service,
            &ticket.table,
            request.into_inner(),
        )
        .await
        .map_err(|error| error.handle_then_to_status())?;

        // https://arrow.apache.org/docs/format/FlightSql.html#:~:text=of%20affected%20rows.-,CommandStatementIngest,-Execute%20a%20bulk
        // > CommandStatementIngest:
        // > return the number of rows ingested via a DoPutUpdateResult message
        Ok(num_ingested)
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

const TICKET_PREFIX: &str = "MangrobeDBTicket:";
impl MangrobeFlightService {
    fn new_ticket_statement_query(&self, statement: &str) -> TicketStatementQuery {
        TicketStatementQuery {
            statement_handle: format!("{TICKET_PREFIX}{statement}").into(),
        }
    }

    fn get_statement(
        &self,
        ticket_statement_query: &TicketStatementQuery,
    ) -> Result<String, Status> {
        let sql = String::from_utf8(ticket_statement_query.statement_handle.to_vec())
            .map_err(|err| Status::invalid_argument(format!("invalid sql: {err}")))?;
        let sql = sql
            .strip_prefix(TICKET_PREFIX)
            .ok_or(Status::invalid_argument(format!("invalid sql: {sql}")))?;

        Ok(sql.into())
    }
}
