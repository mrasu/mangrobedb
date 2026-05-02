use crate::application::import::error::ImportError;
use crate::application::import::importing_records::ImportingRecords;
use crate::domain::port::CatalogPort;
use arrow::record_batch::RecordBatch;

#[derive(Debug)]
pub struct ImportService<R: CatalogPort> {
    catalog_port: R,
}

impl<R: CatalogPort> ImportService<R> {
    pub fn new(catalog_port: R) -> Self {
        Self { catalog_port }
    }

    pub fn import(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<(), ImportError> {
        let table_schema = self.catalog_port.get_table_schema(table_name)?;
        let importing_records = ImportingRecords::try_new(table_schema, batches)?;
        let importing_records =
            importing_records.update_mangrobe_schema_if_required(&self.catalog_port)?;
        let table_records = importing_records.to_table_records()?;

        table_records.print_record_batch()?;

        Ok(())
    }
}
