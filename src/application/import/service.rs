use crate::application::import::error::{ImportError, ImportUserError};
use crate::application::import::importing_records::ImportingRecords;
use crate::domain::table_schema;
use crate::domain::table_schema::{DUMMY_TABLE, TableSchema};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Default)]
pub struct ImportService;

impl ImportService {
    pub fn import(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<(), ImportError> {
        let table_schema = get_table_dummy(table_name)?;
        let importing_records = ImportingRecords::try_new(table_schema, batches)?;
        let table_records = importing_records.to_table_records()?;

        table_records.print_record_batch()?;

        Ok(())
    }
}

// dummy function to get table schema
// TODO: get schema via API
fn get_table_dummy(table_name: &str) -> Result<TableSchema, ImportError> {
    if table_name != DUMMY_TABLE {
        return Err(ImportUserError::InvalidTable {
            table_name: table_name.to_string(),
        }
        .into());
    }

    let res = table_schema::initial_dummy_table_schema();
    Ok(res)
}
