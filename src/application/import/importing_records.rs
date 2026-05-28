use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::application::import::validate::validate_schema;
use crate::domain::column_data_type::{ColumnDataType, TimeUnit};
use crate::domain::flush_unit::FlushUnit;
use crate::domain::flush_unit_record::FlushUnitRecord;
use crate::domain::partition::Partition;
use crate::domain::port::catalog::CatalogPort;
use crate::domain::table_schema::TableSchema;
use anyhow::{Context, anyhow};
use arrow::array::{
    Array, ArrowPrimitiveType, BooleanArray, Int64Array, PrimitiveArray, TimestampMicrosecondArray,
};
use arrow::compute::{concat_batches, filter_record_batch};
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
            .context("validated importing records must have at least one batch")?
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
        let file_unit_records = self.split_by_flush_unit(&self.record_batches)?;

        Ok(file_unit_records)
    }

    fn split_by_flush_unit(
        &self,
        records: &[RecordBatch],
    ) -> Result<Vec<FlushUnitRecord>, ApplicationError> {
        let mut records_by_flush_unit: BTreeMap<FlushUnit, Vec<RecordBatch>> = BTreeMap::new();

        for record in records {
            let stream_column_values = self.schema.stream_array(record)?;

            match self.schema.partition_data_type() {
                ColumnDataType::Timestamp(TimeUnit::Microsecond) => {
                    let partitions = self.schema.partition_array(record)?;

                    for flush_unit in
                        self.flush_units_in_time_record(stream_column_values, partitions)?
                    {
                        let filter =
                            BooleanArray::from_iter((0..record.num_rows()).map(|row_index| {
                                flush_unit.matches(
                                    stream_column_values.value(row_index),
                                    Partition::TimeMicrosecond(partitions.value(row_index)),
                                )
                            }));
                        let filtered_record = filter_record_batch(record, &filter)?;

                        records_by_flush_unit
                            .entry(flush_unit)
                            .or_default()
                            .push(filtered_record);
                    }
                }
                ColumnDataType::Int64 => {
                    let partitions = self.schema.partition_int64_array(record)?;

                    for flush_unit in
                        self.flush_units_in_int64_record(stream_column_values, partitions)?
                    {
                        let filter =
                            BooleanArray::from_iter((0..record.num_rows()).map(|row_index| {
                                flush_unit.matches(
                                    stream_column_values.value(row_index),
                                    Partition::Int64(partitions.value(row_index)),
                                )
                            }));
                        let filtered_record = filter_record_batch(record, &filter)?;

                        records_by_flush_unit
                            .entry(flush_unit)
                            .or_default()
                            .push(filtered_record);
                    }
                }
                data_type => unreachable!("unsupported partition data type: {data_type}"),
            }
        }

        let flush_unit_records = records_by_flush_unit
            .into_iter()
            .map(|(flush_unit, records)| self.create_flush_unit_record(flush_unit, records))
            .collect::<Result<Vec<_>, ApplicationError>>()?;

        Ok(flush_unit_records)
    }

    fn flush_units_in_time_record(
        &self,
        stream_column_values: &Int64Array,
        partitions: &TimestampMicrosecondArray,
    ) -> Result<Vec<FlushUnit>, ApplicationError> {
        let mut flush_units = BTreeSet::new();

        for row_index in 0..stream_column_values.len() {
            self.validate_flush_unit_row(row_index, stream_column_values, partitions)?;

            flush_units.insert(FlushUnit::new(
                stream_column_values.value(row_index),
                Partition::TimeMicrosecond(partitions.value(row_index)),
            ));
        }

        Ok(flush_units.into_iter().collect())
    }

    fn flush_units_in_int64_record(
        &self,
        stream_column_values: &Int64Array,
        partitions: &Int64Array,
    ) -> Result<Vec<FlushUnit>, ApplicationError> {
        let mut flush_units = BTreeSet::new();

        for row_index in 0..stream_column_values.len() {
            self.validate_flush_unit_row(row_index, stream_column_values, partitions)?;

            flush_units.insert(FlushUnit::new(
                stream_column_values.value(row_index),
                Partition::Int64(partitions.value(row_index)),
            ));
        }

        Ok(flush_units.into_iter().collect())
    }

    fn validate_flush_unit_row<T: ArrowPrimitiveType>(
        &self,
        row_index: usize,
        stream_column_values: &Int64Array,
        partitions: &PrimitiveArray<T>,
    ) -> Result<(), ApplicationError> {
        if stream_column_values.is_null(row_index) {
            return Err(anyhow!("internal stream column must not contain null").into());
        }
        if partitions.is_null(row_index) {
            return Err(anyhow!("internal partition column must not contain null").into());
        }

        Ok(())
    }

    fn create_flush_unit_record(
        &self,
        flush_unit: FlushUnit,
        records: Vec<RecordBatch>,
    ) -> Result<FlushUnitRecord, ApplicationError> {
        let schema = records
            .first()
            .context("unexpected empty record batches for flush unit")?
            .schema();
        let record = concat_batches(&schema, records.iter())?;
        Ok(FlushUnitRecord::new(flush_unit, record))
    }
}
