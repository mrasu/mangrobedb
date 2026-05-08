use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::application::import::validate::validate_schema;
use crate::domain::flush_unit::FlushUnit;
use crate::domain::flush_unit_record::FlushUnitRecord;
use crate::domain::port::catalog::CatalogPort;
use crate::domain::table_mapping::{MappingStrategy, TableMapping};
use crate::domain::table_schema::TableSchema;
use anyhow::anyhow;
use arrow::array::{Array, BooleanArray, Int32Array, TimestampMicrosecondArray};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, BTreeSet};
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

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

impl ImportingRecords<Validated> {
    pub fn try_new(
        table_schema: TableSchema,
        record_batches: Vec<RecordBatch>,
    ) -> Result<Self, ApplicationError> {
        let first_record_schema = record_batches
            .first()
            .ok_or(ApplicationUserError::EmptyImport)?
            .schema();

        for batch in &record_batches {
            if batch.schema() != first_record_schema {
                return Err(ApplicationUserError::SchemaMismatch.into());
            }
        }

        validate_schema(&table_schema, &first_record_schema)?;

        Ok(Self::new(table_schema, record_batches))
    }

    pub async fn update_mangrobe_schema_if_required<R: CatalogPort>(
        self,
        port: &Arc<R>,
    ) -> Result<ImportingRecords<MangrobeSchemaUpdated>, ApplicationError> {
        let schema = self
            .record_batches
            .first()
            .expect("validated importing records must have at least one batch")
            .schema();
        let result = self
            .schema
            .add_missing_public_columns_if_required(&schema)?;

        if result.schema_changed {
            port.update_table_schema(&result.schema.table_name, result.schema.clone())
                .await?;
        }

        Ok(ImportingRecords::new(result.schema, self.record_batches))
    }
}

impl ImportingRecords<MangrobeSchemaUpdated> {
    pub fn to_flush_unit_records(&self) -> Result<Vec<FlushUnitRecord>, ApplicationError> {
        let records = self
            .record_batches
            .iter()
            .map(|record| self.add_internal_columns(record))
            .collect::<Result<Vec<_>, _>>()?;

        let file_unit_records = self.split_by_flush_unit(records)?;

        Ok(file_unit_records)
    }

    fn add_internal_columns(&self, batch: &RecordBatch) -> Result<RecordBatch, ApplicationError> {
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
    ) -> Result<(Field, Arc<Int32Array>), ApplicationError> {
        let stream_id_mapping = self.schema.stream_id_mapping();
        let stream_index = schema
            .index_of(&stream_id_mapping.src_column_ref().name)
            .map_err(|_| ApplicationUserError::MissingColumn {
                column_name: stream_id_mapping.src_column_ref().name.clone(),
            })?;

        let stream_ids = batch.column(stream_index);
        let stream_ids = stream_ids
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ApplicationUserError::IncompatibleColumnType {
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

    fn validate_stream_ids(&self, stream_ids: &Int32Array) -> Result<(), ApplicationError> {
        for row_index in 0..stream_ids.len() {
            if stream_ids.is_null(row_index) {
                return Err(ApplicationUserError::UnsupportedStreamId {
                    row_index,
                    value: None,
                }
                .into());
            }

            let value = stream_ids.value(row_index);
            if value != 0 {
                return Err(ApplicationUserError::UnsupportedStreamId {
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
    ) -> Result<(Field, Arc<TimestampMicrosecondArray>), ApplicationError> {
        let partition_time_mapping = self.schema.partition_time_mapping();
        let partition_time_index = schema
            .index_of(&partition_time_mapping.src_column_ref().name)
            .map_err(|_| ApplicationUserError::MissingColumn {
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
    ) -> Result<TimestampMicrosecondArray, ApplicationError> {
        if !matches!(partition_time_mapping.strategy, MappingStrategy::ToHour) {
            return Err(ApplicationUserError::NotImplemented {
                message: "partition_time works only to_hour".into(),
            }
            .into());
        };

        if array.null_count() > 0 {
            return Err(ApplicationUserError::NullValue {
                column_name: partition_time_mapping.src_column_ref().name.to_string(),
            }
            .into());
        }

        Ok(partition_time_mapping
            .strategy
            .create_to_hour_array(array)
            .map_err(|_| ApplicationUserError::IncompatibleColumnType {
                column_name: partition_time_mapping.src_column_ref().name.to_string(),
                expected: "Timestamp".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?)
    }

    fn split_by_flush_unit(
        &self,
        records: Vec<RecordBatch>,
    ) -> Result<Vec<FlushUnitRecord>, ApplicationError> {
        let mut records_by_flush_unit: BTreeMap<FlushUnit, Vec<RecordBatch>> = BTreeMap::new();

        for record in records {
            let stream_ids = self.schema.stream_id_array(&record)?;
            let partition_times = self.schema.partition_time_array(&record)?;

            for flush_unit in self.flush_units_in_record(stream_ids, partition_times)? {
                let filter = BooleanArray::from_iter((0..record.num_rows()).map(|row_index| {
                    flush_unit.matches(
                        stream_ids.value(row_index),
                        partition_times.value(row_index),
                    )
                }));
                let filtered_record = filter_record_batch(&record, &filter)?;

                records_by_flush_unit
                    .entry(flush_unit)
                    .or_default()
                    .push(filtered_record);
            }
        }

        let flush_unit_records = records_by_flush_unit
            .into_iter()
            .map(|(flush_unit, records)| self.create_flush_unit_record(flush_unit, records))
            .collect::<Result<Vec<_>, ApplicationError>>()?;

        Ok(flush_unit_records)
    }

    fn flush_units_in_record(
        &self,
        stream_ids: &Int32Array,
        partition_times: &TimestampMicrosecondArray,
    ) -> Result<Vec<FlushUnit>, ApplicationError> {
        let mut flush_units = BTreeSet::new();

        for row_index in 0..stream_ids.len() {
            if stream_ids.is_null(row_index) {
                return Err(anyhow!("internal stream id column must not contain null").into());
            }
            if partition_times.is_null(row_index) {
                return Err(anyhow!("internal partition time column must not contain null").into());
            }

            flush_units.insert(FlushUnit::new(
                stream_ids.value(row_index),
                partition_times.value(row_index),
            ));
        }

        Ok(flush_units.into_iter().collect())
    }

    fn create_flush_unit_record(
        &self,
        flush_unit: FlushUnit,
        records: Vec<RecordBatch>,
    ) -> Result<FlushUnitRecord, ApplicationError> {
        let schema = records
            .first()
            .expect("unexpected empty record batches for flush unit")
            .schema();
        let record = concat_batches(&schema, records.iter())?;
        Ok(FlushUnitRecord::new(flush_unit, record))
    }
}
