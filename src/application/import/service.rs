use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::application::flusher::service::FlushService;
use crate::application::import::importing_records::ImportingRecords;
use crate::domain::port::catalog::{CatalogError, CatalogPort};
use crate::domain::port::object_store::ObjectStorePort;
use crate::domain::table::Table;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug)]
pub struct ImportService<C: CatalogPort, O: ObjectStorePort> {
    catalog_port: Arc<C>,
    object_store_port: Arc<O>,
    flush_service: Arc<FlushService<C, O>>,
}

impl<C: CatalogPort, O: ObjectStorePort> ImportService<C, O> {
    pub fn new(
        catalog_port: Arc<C>,
        object_store_port: Arc<O>,
        flush_service: Arc<FlushService<C, O>>,
    ) -> Self {
        Self {
            catalog_port,
            object_store_port,
            flush_service,
        }
    }

    pub async fn import(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<i64, ApplicationError> {
        let table = Table::load(self.catalog_port.as_ref(), table_name)
            .await
            .map_err(|e| match e {
                CatalogError::TableNotFound { table_name } => {
                    ApplicationError::User(ApplicationUserError::UnknownTable { table_name })
                }
                _ => e.into(),
            })?;
        if !self.object_store_port.is_accessible(&table.schema.bucket) {
            return Err(ApplicationUserError::S3InaccessibleTable {
                table_name: table.schema.table_name,
            }
            .into());
        }

        let importing_records = ImportingRecords::try_new(table.schema, batches)?;
        let importing_records = importing_records
            .update_mangrobe_schema_if_required(&self.catalog_port)
            .await?;
        let flush_unit_records = importing_records.to_flush_unit_records()?;

        let mut num_imported = 0i64;
        for flush_unit_record in flush_unit_records {
            self.flush_service
                .accept(importing_records.schema(), &flush_unit_record)
                .await?;
            num_imported += flush_unit_record.batch_record().num_rows() as i64;
        }

        Ok(num_imported)
    }
}
