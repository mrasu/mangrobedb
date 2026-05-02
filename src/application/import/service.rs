use crate::application::import::error::ImportError;
use crate::application::import::importing_records::ImportingRecords;
use crate::domain::repository::TableRepository;
use arrow::record_batch::RecordBatch;

#[derive(Debug)]
pub struct ImportService<R: TableRepository> {
    table_repository: R,
}

impl<R: TableRepository> ImportService<R> {
    pub fn new(table_repository: R) -> Self {
        Self { table_repository }
    }

    pub fn import(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<(), ImportError> {
        let table_schema = self.table_repository.get_table_schema(table_name)?;
        let importing_records = ImportingRecords::try_new(table_schema, batches)?;
        let importing_records =
            importing_records.update_mangrobe_schema_if_required(&self.table_repository)?;
        let table_records = importing_records.to_table_records()?;

        table_records.print_record_batch()?;

        Ok(())
    }
}
