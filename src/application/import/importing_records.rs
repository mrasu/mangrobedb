use crate::application::import::error::{ImportError, ImportUserError};
use crate::application::import::validate::validate_schema;
use crate::domain::repository::TableRepository;
use crate::domain::table_mapping::{MappingStrategy, TableMapping};
use crate::domain::table_records::TableRecords;
use crate::domain::table_schema::TableSchema;
use arrow::array::{Array, Int32Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct Validated;

pub struct MangrobeSchemaUpdated;

pub struct ImportingRecords<State> {
    schema: TableSchema,
    record_batches: Vec<RecordBatch>,
    _state: PhantomData<State>,
}

impl<State> ImportingRecords<State> {
    fn new(schema: TableSchema, record_batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            record_batches,
            _state: PhantomData,
        }
    }
}

impl ImportingRecords<Validated> {
    pub fn try_new(
        table_schema: TableSchema,
        record_batches: Vec<RecordBatch>,
    ) -> Result<Self, ImportError> {
        let first_record_schema = record_batches
            .first()
            .ok_or(ImportUserError::EmptyImport)?
            .schema();

        for batch in &record_batches {
            if batch.schema() != first_record_schema {
                return Err(ImportUserError::SchemaMismatch.into());
            }
        }

        validate_schema(&table_schema, &first_record_schema)?;

        Ok(Self::new(table_schema, record_batches))
    }

    pub fn update_mangrobe_schema_if_required<R: TableRepository>(
        self,
        repository: &R,
    ) -> Result<ImportingRecords<MangrobeSchemaUpdated>, ImportError> {
        let schema = self
            .record_batches
            .first()
            .expect("validated importing records must have at least one batch")
            .schema();
        let result = self
            .schema
            .add_missing_public_columns_if_required(&schema)?;

        if result.schema_changed {
            repository.update_table_schema(&result.schema.name, result.schema.clone())?;
        }

        Ok(ImportingRecords::new(result.schema, self.record_batches))
    }
}

impl ImportingRecords<MangrobeSchemaUpdated> {
    pub fn to_table_records(&self) -> Result<TableRecords, ImportError> {
        let records = self
            .record_batches
            .iter()
            .map(|record| self.add_internal_columns(record))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TableRecords::new(self.schema.clone(), records))
    }

    fn add_internal_columns(&self, batch: &RecordBatch) -> Result<RecordBatch, ImportError> {
        let schema = batch.schema();
        let (internal_stream_id_field, internal_stream_ids) =
            self.create_internal_stream_ids(&schema, batch)?;
        let (internal_partition_time_field, internal_partition_times) =
            self.create_internal_partition_times(&schema, batch)?;

        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(internal_stream_id_field));
        fields.push(Arc::new(internal_partition_time_field));

        let mut columns = batch.columns().to_vec();
        columns.push(internal_stream_ids);
        columns.push(internal_partition_times);

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn create_internal_stream_ids(
        &self,
        schema: &SchemaRef,
        batch: &RecordBatch,
    ) -> Result<(Field, Arc<Int32Array>), ImportError> {
        let stream_id_mapping = self.schema.stream_id_mapping();
        let stream_index = schema
            .index_of(&stream_id_mapping.src_column_ref().name)
            .map_err(|_| ImportUserError::MissingColumn {
                column_name: stream_id_mapping.src_column_ref().name.clone(),
            })?;

        let stream_ids = batch.column(stream_index);
        let stream_ids = stream_ids
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ImportUserError::IncompatibleColumnType {
                column_name: stream_id_mapping.src_column_ref().name.clone(),
                expected: "Int32".to_string(),
                actual: format!("{:?}", stream_ids.data_type()),
            })?;

        self.validate_stream_ids(stream_ids)?;

        let internal_stream_ids = Arc::new(stream_ids.clone());
        let field = Field::new(
            stream_id_mapping.dst_column_ref().name.clone(),
            DataType::Int32,
            false,
        );

        Ok((field, internal_stream_ids))
    }

    fn validate_stream_ids(&self, stream_ids: &Int32Array) -> Result<(), ImportError> {
        for row_index in 0..stream_ids.len() {
            if stream_ids.is_null(row_index) {
                return Err(ImportUserError::UnsupportedStreamId {
                    row_index,
                    value: None,
                }
                .into());
            }

            let value = stream_ids.value(row_index);
            if value != 0 {
                return Err(ImportUserError::UnsupportedStreamId {
                    row_index,
                    value: Some(value),
                }
                .into());
            }
        }

        Ok(())
    }

    fn create_internal_partition_times(
        &self,
        schema: &SchemaRef,
        batch: &RecordBatch,
    ) -> Result<(Field, Arc<TimestampMicrosecondArray>), ImportError> {
        let partition_time_mapping = self.schema.partition_time_mapping();
        let partition_time_index = schema
            .index_of(&partition_time_mapping.src_column_ref().name)
            .map_err(|_| ImportUserError::MissingColumn {
                column_name: partition_time_mapping.src_column_ref().name.clone(),
            })?;

        let internal_partition_time_array = self.create_internal_partition_time_array(
            partition_time_mapping,
            batch.column(partition_time_index).as_ref(),
        )?;

        let field = Field::new(
            partition_time_mapping.dst_column_ref().name.clone(),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        );

        Ok((field, Arc::new(internal_partition_time_array)))
    }

    fn create_internal_partition_time_array<T: Array + ?Sized>(
        &self,
        partition_time_mapping: &TableMapping,
        array: &T,
    ) -> Result<TimestampMicrosecondArray, ImportError> {
        if !matches!(partition_time_mapping.strategy, MappingStrategy::ToHour) {
            return Err(ImportUserError::NotImplemented {
                message: "partition_time works only to_hour".into(),
            }
            .into());
        };

        if array.null_count() > 0 {
            return Err(ImportUserError::NullValue {
                column_name: partition_time_mapping.src_column_ref().name.to_string(),
            }
            .into());
        }

        Ok(partition_time_mapping
            .strategy
            .create_to_hour_array(array)
            .map_err(|_| ImportUserError::IncompatibleColumnType {
                column_name: partition_time_mapping.src_column_ref().name.to_string(),
                expected: "Timestamp".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?)
    }
}
