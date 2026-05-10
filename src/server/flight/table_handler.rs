use crate::domain::port::catalog::TableSummary;
use crate::server::flight::error::FlightServerError;
use crate::server::flight::server::{DoGetStream, SharedQueryService};
use anyhow::anyhow;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_flight::error::FlightError;
use arrow_flight::sql::metadata::GetTablesBuilder;
use arrow_flight::sql::{CommandGetTables, ProstMessageExt};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::stream;
use prost::Message;
use tonic::{Request, Response, Status};

pub(super) struct TableHandler {
    query_service: SharedQueryService,
}

impl TableHandler {
    pub fn new(query_service: SharedQueryService) -> Self {
        Self { query_service }
    }

    pub async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, FlightServerError> {
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

    pub async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, FlightServerError> {
        if query.include_schema {
            return Err(FlightServerError::unimplemented(
                "CommandGetTables include_schema is not supported",
            ));
        }

        let tables = self
            .query_service
            .list_tables()
            .await
            .map_err(|error| FlightServerError::from(error).handle_then_to_status())?;

        let table_batch = self.build_tables(query.into_builder(), &tables)?;

        let flight_data = batches_to_flight_data(table_batch.schema().as_ref(), vec![table_batch])
            .map_err(|error| Status::internal(anyhow!(error).to_string()))?;
        let output = stream::iter(flight_data.into_iter().map(Ok));

        Ok(Response::new(Box::pin(output)))
    }

    fn build_tables(
        &self,
        mut builder: GetTablesBuilder,
        tables: &[TableSummary],
    ) -> Result<RecordBatch, FlightError> {
        let table_schema = Schema::empty();
        for table in tables {
            builder
                .append(
                    table.catalog_name(),
                    table.schema_name(),
                    table.table_name.clone(),
                    table.table_type(),
                    &table_schema,
                )
                .map_err(Status::from)?;
        }

        builder.build()
    }
}
