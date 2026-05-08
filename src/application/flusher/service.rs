use crate::application::error::ApplicationError;
use crate::domain::common_ports::CommonPorts;
use crate::domain::flush_unit::FlushUnit;
use crate::domain::flush_unit_record::FlushUnitRecord;
use crate::domain::port::catalog::{AddFile, AddFilesEntry, CatalogPort, FileMetadata};
use crate::domain::port::object_store::ObjectStorePort;
use crate::domain::table::Table;
use crate::domain::table_schema::TableSchema;
use crate::domain::vortex_file_record::VortexFileRecord;
use crate::infrastructure::vortex::writer::write_vortex_file;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug)]
pub struct FlushService<C: CatalogPort, O: ObjectStorePort> {
    catalog_port: Arc<C>,
    object_store_port: Arc<O>,
    common_ports: Arc<CommonPorts>,
    flush_interval: Duration,
    flush_once_lock: Mutex<()>,
    buffer: Mutex<BTreeMap<StreamBufferKey, Vec<RecordBatch>>>,
    uploading_buffer: Mutex<Vec<UploadingBufferEntry>>,
}

impl<C: CatalogPort, O: ObjectStorePort> FlushService<C, O> {
    pub fn new(
        catalog_port: Arc<C>,
        object_store_port: Arc<O>,
        common_ports: Arc<CommonPorts>,
        flush_interval: Duration,
    ) -> Self {
        Self {
            catalog_port,
            object_store_port,
            common_ports,
            flush_interval,
            flush_once_lock: Mutex::new(()),
            buffer: Mutex::new(BTreeMap::new()),
            uploading_buffer: Mutex::new(Vec::new()),
        }
    }

    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    pub async fn accept(
        &self,
        table_schema: &TableSchema,
        flush_unit_record: &FlushUnitRecord,
    ) -> Result<(), ApplicationError> {
        let key = StreamBufferKey {
            table_name: table_schema.table_name.clone(),
            flush_unit: *flush_unit_record.flush_unit(),
        };
        self.buffer
            .lock()
            .await
            .entry(key)
            .or_default()
            .push(flush_unit_record.batch_record().clone());

        Ok(())
    }

    pub async fn flush_once(&self) {
        let _flush_once_guard = match self.flush_once_lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // Wait for the in-flight flush, but skip duplicate work.
                let _ = self.flush_once_lock.lock().await;
                return;
            }
        };

        self.move_buffer_to_uploading_buffer().await;
        self.upload_uploading_buffer_entries().await;
    }

    async fn move_buffer_to_uploading_buffer(&self) {
        let stream_buffers = self.take_buffer_entries().await;
        let mut uploading_entries = Vec::new();

        for (key, records) in stream_buffers {
            uploading_entries.push(UploadingBufferEntry {
                table_name: key.table_name,
                flush_unit: key.flush_unit,
                file_id: self.common_ports.uuid_generator.generate(),
                records,
            });
        }

        self.uploading_buffer.lock().await.extend(uploading_entries);
    }

    async fn take_buffer_entries(&self) -> BTreeMap<StreamBufferKey, Vec<RecordBatch>> {
        let mut buffer = self.buffer.lock().await;
        std::mem::take(&mut *buffer)
    }

    async fn upload_uploading_buffer_entries(&self) {
        let uploading_entries = self.take_uploading_buffer_entries().await;
        let mut failed_entries = Vec::new();

        for uploading_entry in uploading_entries {
            let idempotency_key = uploading_entry.file_id.as_bytes();
            if let Err(error) = self.upload(idempotency_key, &uploading_entry).await {
                error!(
                    table_name = %uploading_entry.table_name,
                    flush_unit = ?uploading_entry.flush_unit,
                    file_id = %uploading_entry.file_id,
                    "failed to upload buffered records: {error}"
                );
                failed_entries.push(uploading_entry);
            }
        }

        self.push_back_uploading_buffer_entries(failed_entries)
            .await;
    }

    async fn take_uploading_buffer_entries(&self) -> Vec<UploadingBufferEntry> {
        let mut uploading_buffer = self.uploading_buffer.lock().await;
        std::mem::take(&mut *uploading_buffer)
    }

    async fn push_back_uploading_buffer_entries(&self, failed_entries: Vec<UploadingBufferEntry>) {
        self.uploading_buffer.lock().await.extend(failed_entries);
    }

    async fn upload(
        &self,
        idempotency_key: &[u8],
        uploading_entry: &UploadingBufferEntry,
    ) -> Result<(), ApplicationError> {
        if uploading_entry.records.is_empty() {
            info!("skip upload because stream buffer is empty");
            return Ok(());
        }

        let table = Table::load(self.catalog_port.as_ref(), &uploading_entry.table_name).await?;
        let file_record = uploading_entry.to_vortex_file_record()?;
        let write_result = write_vortex_file(&file_record).await?;
        let path = file_record.path()?;

        self.object_store_port.upload(
            &table.schema.table_name,
            &table.schema.bucket,
            &table.schema.path_prefix,
            &path,
            write_result.temp_file.path(),
        )?;

        let entries = vec![AddFilesEntry {
            partition_time: file_record.partition_time_micros(),
            files: vec![AddFile {
                path,
                size: write_result.file_size,
                column_statistics: write_result.statistics,
                file_metadata: FileMetadata::default(),
            }],
        }];
        self.catalog_port
            .add_files(
                idempotency_key,
                &table.schema.table_name,
                uploading_entry.flush_unit.stream_id.into(),
                entries,
            )
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamBufferKey {
    table_name: String,
    flush_unit: FlushUnit,
}

#[derive(Debug)]
struct UploadingBufferEntry {
    table_name: String,
    flush_unit: FlushUnit,
    file_id: Uuid,
    records: Vec<RecordBatch>,
}

impl UploadingBufferEntry {
    fn to_vortex_file_record(&self) -> Result<VortexFileRecord, ApplicationError> {
        let schema = self
            .records
            .first()
            .expect("uploading records must not be empty")
            .schema();
        let record = concat_batches(&schema, self.records.iter())?;

        Ok(VortexFileRecord::new(
            self.vortex_file_name(),
            self.flush_unit,
            record,
        ))
    }

    fn vortex_file_name(&self) -> String {
        format!("{}.vortex", self.file_id)
    }
}
