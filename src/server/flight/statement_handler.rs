use crate::server::flight::error::FlightServerError;
use crate::server::flight::server::{DoGetStream, SharedImportService, SharedQueryService};
use anyhow::anyhow;
use arrow::array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    CommandStatementIngest, CommandStatementQuery, ProstMessageExt, TicketStatementQuery,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status};

pub(super) struct StatementHandler {
    import_service: SharedImportService,
    query_service: SharedQueryService,
}

impl StatementHandler {
    pub fn new(import_service: SharedImportService, query_service: SharedQueryService) -> Self {
        Self {
            import_service,
            query_service,
        }
    }

    pub async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, FlightServerError> {
        let ticket = Ticket {
            ticket: new_ticket_statement_query(&query.query)
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

    pub async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, FlightServerError> {
        let sql = pick_query_from_ticket_statement_query(&ticket)?;

        let query_output = self
            .query_service
            .query(&sql)
            .await
            .map_err(FlightServerError::from)?;

        let flight_data =
            batches_to_flight_data(query_output.schema.as_ref(), query_output.batches)
                .map_err(|error| FlightServerError::internal(anyhow!(error)))?;

        let output = stream::iter(flight_data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(output)))
    }

    pub async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, FlightServerError> {
        if ticket.table_definition_options.is_some() {
            return Err(FlightServerError::invalid_argument(
                "table_definition_options like if_not_exist is not supported",
            ));
        }

        let table_name = ticket.table;

        let stream = request.into_inner();
        let mut record_batches =
            FlightRecordBatchStream::new_from_flight_data(stream.map_err(Into::into));

        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = record_batches.next().await {
            let batch = batch.map_err(|error| {
                FlightServerError::invalid_argument(format!(
                    "failed to decode Arrow Flight data: {error}"
                ))
            })?;
            batches.push(batch);
        }

        let num_ingested = self
            .import_service
            .import(&table_name, batches)
            .await
            .map_err(FlightServerError::from)?;

        // https://arrow.apache.org/docs/format/FlightSql.html#:~:text=of%20affected%20rows.-,CommandStatementIngest,-Execute%20a%20bulk
        // > CommandStatementIngest:
        // > return the number of rows ingested via a DoPutUpdateResult message
        Ok(num_ingested)
    }
}

const TICKET_PREFIX: &str = "MangrobeDBTicket:";

fn new_ticket_statement_query(statement: &str) -> TicketStatementQuery {
    TicketStatementQuery {
        statement_handle: format!("{TICKET_PREFIX}{statement}").into(),
    }
}

fn pick_query_from_ticket_statement_query(
    ticket_statement_query: &TicketStatementQuery,
) -> Result<String, Status> {
    let sql = String::from_utf8(ticket_statement_query.statement_handle.to_vec())
        .map_err(|err| Status::invalid_argument(format!("invalid sql: {err}")))?;
    let sql = sql
        .strip_prefix(TICKET_PREFIX)
        .ok_or(Status::invalid_argument(format!("invalid sql: {sql}")))?;

    Ok(sql.into())
}
