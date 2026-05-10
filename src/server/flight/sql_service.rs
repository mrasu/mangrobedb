use crate::server::flight::server::{SharedImportService, SharedQueryService};
use crate::server::flight::statement_handler::StatementHandler;
use crate::server::flight::table_handler::TableHandler;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    CommandGetTables, CommandStatementIngest, CommandStatementQuery, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, FlightInfo, Ticket};
use tonic::{Request, Response, Status};

pub(super) struct SqlService {
    statement_handler: StatementHandler,
    table_handler: TableHandler,
}

impl SqlService {
    pub fn new(import_service: SharedImportService, query_service: SharedQueryService) -> Self {
        Self {
            statement_handler: StatementHandler::new(import_service.clone(), query_service.clone()),
            table_handler: TableHandler::new(query_service.clone()),
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for SqlService {
    type FlightService = Self;

    /// Get a FlightInfo for executing a SQL query.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.statement_handler
            .get_flight_info_statement(query, request)
            .await
            .map_err(|error| error.handle_then_to_status())
    }

    /// Get a FlightInfo for listing tables.
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.table_handler
            .get_flight_info_tables(query, request)
            .await
            .map_err(|error| error.handle_then_to_status())
    }

    /// Get a FlightDataStream containing the query results.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.statement_handler
            .do_get_statement(ticket, request)
            .await
            .map_err(|error| error.handle_then_to_status())
    }

    /// Get a FlightDataStream containing the list of tables.
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.table_handler
            .do_get_tables(query, request)
            .await
            .map_err(|error| error.handle_then_to_status())
    }

    /// Execute a bulk ingestion.
    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.statement_handler
            .do_put_statement_ingest(ticket, request)
            .await
            .map_err(|error| error.handle_then_to_status())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
