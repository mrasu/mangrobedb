use crate::application::import::error::ImportError;
use crate::application::import::importing_records::ImportingRecords;
use crate::di::Container;
use crate::domain::port::CatalogPort;
use crate::infrastructure::vortex::writer::write_vortex_file;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug)]
pub struct ImportService<R: CatalogPort> {
    catalog_port: R,
    container: Arc<Container>,
}

impl<R: CatalogPort> ImportService<R> {
    pub fn new(catalog_port: R, container: Arc<Container>) -> Self {
        Self {
            catalog_port,
            container,
        }
    }

    pub fn import(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<(), ImportError> {
        let table_schema = self.catalog_port.get_table_schema(table_name)?;
        let importing_records = ImportingRecords::try_new(table_schema, batches)?;
        let importing_records =
            importing_records.update_mangrobe_schema_if_required(&self.catalog_port)?;
        let file_batch = importing_records.to_file_batch(self.container.uuid_generator.as_ref())?;

        file_batch.print_record_batch()?;
        for file_record in file_batch.file_records() {
            let write_result = write_vortex_file(file_record)?;
            println!(
                "vortex_temp_file={}",
                write_result.temp_file.path().display()
            );
            println!("path={}", file_record.path()?);
            println!("vortex_statistics={:?}", write_result.statistics);
        }

        Ok(())
    }
}
