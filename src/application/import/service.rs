use crate::application::import::error::ImportError;
use crate::application::import::importing_records::ImportingRecords;
use crate::domain::common_ports::CommonPorts;
use crate::domain::file_batch::FileBatch;
use crate::domain::port::catalog::{AddFile, AddFilesEntry, CatalogPort};
use crate::domain::port::object_store::ObjectStorePort;
use crate::infrastructure::vortex::writer::write_vortex_file;
use anyhow::anyhow;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct ImportService<C: CatalogPort, O: ObjectStorePort> {
    catalog_port: Arc<C>,
    object_store_port: Arc<O>,
    container: Arc<CommonPorts>,
}

impl<C: CatalogPort, O: ObjectStorePort> ImportService<C, O> {
    pub fn new(
        catalog_port: Arc<C>,
        object_store_port: Arc<O>,
        container: Arc<CommonPorts>,
    ) -> Self {
        Self {
            catalog_port,
            object_store_port,
            container,
        }
    }

    pub fn import(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<(), ImportError> {
        let table_schema = self.catalog_port.get_table_schema(table_name)?;
        let importing_records = ImportingRecords::try_new(table_schema, batches)?;
        let importing_records =
            importing_records.update_mangrobe_schema_if_required(&self.catalog_port)?;
        let file_batch = importing_records.to_file_batch(self.container.uuid_generator.as_ref())?;

        self.upload(&file_batch)?;

        Ok(())
    }

    // TODO: replace with flusher.
    fn upload(&self, file_batch: &FileBatch) -> Result<(), ImportError> {
        let table_name = file_batch.schema().name.clone();

        let mut stream_id = None;
        let mut files_by_partition_time: BTreeMap<i64, Vec<AddFile>> = BTreeMap::new();

        for file_record in file_batch.file_records() {
            let write_result = write_vortex_file(file_record)?;
            let path = file_record.path()?;
            self.object_store_port
                .upload(&table_name, &path, write_result.temp_file.path())?;

            let current_stream_id = file_record.stream_id();
            match stream_id {
                None => stream_id = Some(current_stream_id),
                Some(existing) if existing != current_stream_id => {
                    return Err(anyhow!(
                        "mixed stream_id in one import is not supported: {existing} and {current_stream_id}"
                    )
                        .into());
                }
                Some(_) => {}
            }

            files_by_partition_time
                .entry(file_record.partition_time_micros())
                .or_default()
                .push(AddFile {
                    path,
                    size: write_result.file_size,
                    column_statistics: write_result.statistics,
                });
        }

        let stream_id = stream_id.ok_or_else(|| anyhow!("file batch is empty"))?;
        let entries = files_by_partition_time
            .into_iter()
            .map(|(partition_time, files)| AddFilesEntry {
                partition_time,
                files,
            })
            .collect();
        self.catalog_port
            .add_files(&table_name, stream_id, entries)?;

        Ok(())
    }
}
