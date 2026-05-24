use crate::domain::column_data_type::{ColumnDataType, TimeUnit};
use crate::domain::file::{
    File, FileColumnStatisticsType, FileFormat, FileInfo, FileMetadata, FileMetadataType,
};
use crate::domain::partition::Partition;
use crate::domain::partition_filter::{PartitionFilter, PartitionPredicate};
use crate::domain::partition_range::{BoundInclusivity, PartitionRangeBound};
use crate::domain::port::catalog::{
    AddFilesEntry, CatalogError, CatalogPort, CreateTableRequest as CatalogCreateTableRequest,
    TableSummary as CatalogTableSummary,
};
use crate::domain::statistics::{ColumnStatistics, StatisticValue};
use crate::domain::table_schema::{ExternalLocation, PublicColumnDefinition, TableSchema};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use mangrobe_api_server::proto::statistics_value::Value;
use mangrobe_api_server::proto::{
    data_type, partition_predicate,
    partition_value, AddFileEntry as MangrobeAddFileEntry, AddFileInfoEntry as MangrobeAddFileInfoEntry,
    AddFilesRequest,
    BoundInclusivity as MangrobeBoundInclusivity, Column as MangrobeColumn,
    ColumnStatisticsEntry as MangrobeColumnStatisticsEntry, CreateTableRequest as MangrobeCreateTableRequest,
    DataType as MangrobeDataType, EvolveTableSchemaRequest,
    ExternalLocation as MangrobeExternalLocation, FileColumnStatisticsType as MangrobeFileColumnStatisticsType,
    FileFormat as MangrobeFileFormat, FileMetadataEntry as MangrobeFileMetadataEntry, FileMetadataType as MangrobeFileMetadataType,
    GetCurrentStateRequest, GetFileInfoRequest,
    GetTableRequest as MangrobeGetTableRequest, IdempotencyKey,
    ListTablesRequest as MangrobeListTablesRequest, PartitionBound as MangrobePartitionBound,
    PartitionDataType as MangrobePartitionDataType, PartitionField as MangrobePartitionField, PartitionFilter as MangrobePartitionFilter,
    PartitionIn, PartitionPredicate as MangrobePartitionPredicate,
    PartitionRange, PartitionTransform as MangrobePartitionTransform, PartitionValue,
    ScalarType as MangrobeScalarType, StatisticsValue,
    StorageScheme as MangrobeStorageScheme, StreamDataType as MangrobeStreamDataType, StreamField as MangrobeStreamField,
    TableDefinition as MangrobeTableDefinition, TableIdentifier, TimeUnit as MangrobeTimeUnit, TimestampType as MangrobeTimestampType,
};
use mangrobe_api_server::Mangrobe;
use prost_types::Timestamp;
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use std::fmt;

// TODO: override in config
pub const MANGROBEDB_CATALOG_NAME: &str = "mangrobedb";
pub const MANGROBEDB_SCHEMA_NAME: &str = "default";

pub struct MangrobeCatalog {
    mangrobe: Mangrobe,
}

impl MangrobeCatalog {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            mangrobe: Mangrobe::new_with_connection(db),
        }
    }
}

impl fmt::Debug for MangrobeCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MangrobeCatalog").finish_non_exhaustive()
    }
}

#[async_trait]
impl CatalogPort for MangrobeCatalog {
    async fn create_table(&self, request: CatalogCreateTableRequest) -> Result<(), CatalogError> {
        let param = MangrobeCreateTableRequest {
            table: Some(to_mangrobe_table_definition(request.table)),
            skip_if_exists: request.skip_if_exists,
        };

        self.mangrobe.data_definition().create_table(param).await?;

        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<CatalogTableSummary>, CatalogError> {
        let param = MangrobeListTablesRequest {
            catalog_name: Some(MANGROBEDB_CATALOG_NAME.into()),
            schema_name: Some(MANGROBEDB_SCHEMA_NAME.into()),
        };
        let response = self.mangrobe.data_definition().list_tables(param).await?;

        response
            .tables
            .into_iter()
            .map(|table| {
                let identifier = table
                    .identifier
                    .context("Mangrobe API returned table summary without identifier")?;
                Ok(CatalogTableSummary {
                    table_name: identifier.table_name,
                    comment: table.comment,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(CatalogError::from)
    }

    async fn get_table(&self, table_name: &str) -> Result<TableSchema, CatalogError> {
        let param = MangrobeGetTableRequest {
            identifier: Some(to_mangrobe_table_identifier(table_name)),
        };
        let response = self.mangrobe.data_definition().get_table(param).await?;
        let table = response
            .table
            .context("Mangrobe API returned get_table response without table")?;

        from_mangrobe_table_definition(table).map_err(CatalogError::from)
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogError> {
        let param = MangrobeGetTableRequest {
            identifier: Some(to_mangrobe_table_identifier(table_name)),
        };
        let response = self.mangrobe.data_definition().get_table(param).await?;
        let table = response
            .table
            .context("Mangrobe API returned get_table response without table")?;

        let table_schema = from_mangrobe_table_definition(table).map_err(CatalogError::from)?;
        Ok(table_schema)
    }

    async fn get_current_state(
        &self,
        table_name: &str,
        stream: i64,
        partition_filter: &PartitionFilter,
    ) -> Result<Vec<File>, CatalogError> {
        let param = GetCurrentStateRequest {
            table_identifier: Some(to_mangrobe_table_identifier(table_name)),
            stream,
            partition_filter: Some(to_mangrobe_partition_filter(partition_filter)),
        };
        let response = self
            .mangrobe
            .data_manipulation()
            .get_current_state(param)
            .await?;

        let mut files = Vec::new();
        for current_partition in response.partitions {
            let partition = from_mangrobe_partition_value(
                current_partition
                    .partition
                    .as_ref()
                    .context("Mangrobe API returned partition without partition")?,
            )?;
            for file in current_partition.files {
                files.push(File {
                    file_id: file.file_id,
                    partition,
                    path: file.path,
                    size: u64::try_from(file.size)
                        .context("Mangrobe API returned negative file size")?,
                });
            }
        }

        Ok(files)
    }

    async fn get_file_info(
        &self,
        table_name: &str,
        file_ids: &[String],
        included_column_statistics_types: &[FileColumnStatisticsType],
        included_file_metadata_types: &[FileMetadataType],
    ) -> Result<HashMap<String, FileInfo>, CatalogError> {
        #[allow(
            clippy::needless_update,
            reason = "Keep the default update so this remains valid when the type is extended."
        )]
        let param = GetFileInfoRequest {
            table_identifier: Some(to_mangrobe_table_identifier(table_name)),
            file_ids: file_ids.to_vec(),
            included_column_statistics_types: included_column_statistics_types
                .iter()
                .copied()
                .map(to_mangrobe_column_statistics_type)
                .collect(),
            included_file_metadata_types: included_file_metadata_types
                .iter()
                .copied()
                .map(to_mangrobe_metadata_type)
                .collect(),
        };
        let response = self
            .mangrobe
            .data_manipulation()
            .get_file_info(param)
            .await?;

        response
            .file_info
            .into_iter()
            .map(|file| {
                let file_id = file.file_id;
                Ok((
                    file_id.clone(),
                    FileInfo {
                        file_id,
                        path: file.path,
                        size: u64::try_from(file.size)
                            .context("Mangrobe API returned negative file size")?,
                        column_statistics: file
                            .column_statistics
                            .into_iter()
                            .map(|statistics| ColumnStatistics {
                                column_name: statistics.column_name,
                                min: statistics.min.and_then(from_mangrobe_statistics_value),
                                max: statistics.max.and_then(from_mangrobe_statistics_value),
                            })
                            .collect(),
                        file_metadata: FileMetadata {
                            parquet_metadata: file
                                .file_metadata
                                .and_then(|metadata| metadata.parquet_metadata),
                        },
                    },
                ))
            })
            .collect::<anyhow::Result<HashMap<_, _>>>()
            .map_err(CatalogError::from)
    }

    async fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError> {
        let proposed_columns = schema
            .public_columns()
            .iter()
            .map(|column| {
                Ok(to_mangrobe_column(PublicColumnDefinition::new(
                    column.name.clone(),
                    column.data_type.clone(),
                    true,
                    None,
                )))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let param = EvolveTableSchemaRequest {
            identifier: Some(to_mangrobe_table_identifier(table_name)),
            proposed_columns,
        };

        self.mangrobe
            .data_definition()
            .evolve_table_schema(param)
            .await?;

        Ok(())
    }

    async fn add_files(
        &self,
        idempotency_key: &[u8],
        table_name: &str,
        stream: i64,
        entries: Vec<AddFilesEntry>,
    ) -> Result<(), CatalogError> {
        let add_file_entries = entries
            .into_iter()
            .map(to_mangrobe_add_file_entry)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let param = AddFilesRequest {
            idempotency_key: Some(IdempotencyKey {
                key: idempotency_key.to_vec(),
            }),
            table_identifier: Some(to_mangrobe_table_identifier(table_name)),
            stream,
            add_file_entries,
        };

        self.mangrobe.data_manipulation().add_files(param).await?;

        Ok(())
    }
}

fn to_mangrobe_table_identifier(table_name: &str) -> TableIdentifier {
    TableIdentifier {
        catalog_name: MANGROBEDB_CATALOG_NAME.into(),
        schema_name: MANGROBEDB_SCHEMA_NAME.into(),
        table_name: table_name.into(),
    }
}

fn from_mangrobe_table_definition(table: MangrobeTableDefinition) -> anyhow::Result<TableSchema> {
    let identifier = table
        .identifier
        .context("Mangrobe API returned table without identifier")?;

    Ok(TableSchema::try_new(
        identifier.table_name,
        from_mangrobe_external_location(
            table
                .location
                .context("Mangrobe API returned table without location")?,
        )?,
        from_mangrobe_file_format(table.format)?,
        table
            .columns
            .into_iter()
            .map(from_mangrobe_column)
            .collect::<anyhow::Result<Vec<_>>>()?,
        from_mangrobe_stream_field(
            table
                .stream_field
                .context("Mangrobe API returned table without stream_column")?,
        )?,
        from_mangrobe_partition_field(
            table
                .partition_field
                .context("Mangrobe API returned table without partition_field")?,
        )?,
        table.comment,
    )?)
}

fn from_mangrobe_external_location(
    location: MangrobeExternalLocation,
) -> anyhow::Result<ExternalLocation> {
    let storage_scheme = MangrobeStorageScheme::try_from(location.storage_scheme)
        .context("Mangrobe API returned invalid storage scheme")?;
    if storage_scheme != MangrobeStorageScheme::S3 {
        return Err(anyhow!(
            "Mangrobe API returned unsupported storage scheme: {storage_scheme:?}"
        ));
    }

    Ok(ExternalLocation {
        bucket: location
            .bucket
            .context("Mangrobe API returned S3 location without bucket")?,
        prefix: location.prefix.unwrap_or_default(),
        endpoint: location.endpoint,
        region: location.region,
    })
}

fn from_mangrobe_column(column: MangrobeColumn) -> anyhow::Result<PublicColumnDefinition> {
    Ok(PublicColumnDefinition::new(
        column.name,
        from_mangrobe_data_type(
            column
                .data_type
                .context("Mangrobe API returned column without data_type")?,
        )?,
        column.nullable,
        column.comment,
    ))
}

fn from_mangrobe_partition_field(field: MangrobePartitionField) -> anyhow::Result<String> {
    if field.transform != MangrobePartitionTransform::Identity as i32 {
        return Err(anyhow!(
            "Mangrobe API returned invalid partition format. Only Identity is supported"
        ));
    }
    if field.dst_column.is_some() {
        return Err(anyhow!(
            "Mangrobe API returned invalid partition format. dst_column is not supported"
        ));
    }
    match MangrobePartitionDataType::try_from(field.result_type)
        .context("Mangrobe API returned invalid partition data type")?
    {
        MangrobePartitionDataType::TimeMicrosecond | MangrobePartitionDataType::Int64 => {}
        other => {
            return Err(anyhow!(
                "Mangrobe API returned unsupported partition data type: {other:?}"
            ));
        }
    }

    Ok(field.src_column)
}

fn from_mangrobe_stream_field(field: MangrobeStreamField) -> anyhow::Result<String> {
    if field.transform != MangrobePartitionTransform::Identity as i32 {
        return Err(anyhow!(
            "Mangrobe API returned invalid stream format. Only Identity is supported"
        ));
    }
    if field.dst_column.is_some() {
        return Err(anyhow!(
            "Mangrobe API returned invalid stream format. No dst_column is allowed"
        ));
    }
    if field.result_type != MangrobeStreamDataType::Int64 as i32 {
        return Err(anyhow!(
            "Mangrobe API returned invalid stream format. Only int64 is supported"
        ));
    }

    Ok(field.src_column)
}

fn from_mangrobe_file_format(value: i32) -> anyhow::Result<FileFormat> {
    match MangrobeFileFormat::try_from(value)
        .context("Mangrobe API returned invalid file format")?
    {
        MangrobeFileFormat::Vortex => Ok(FileFormat::Vortex),
        other => Err(anyhow!(
            "Mangrobe API returned unsupported file format: {other:?}"
        )),
    }
}

fn from_mangrobe_statistics_value(value: StatisticsValue) -> Option<StatisticValue> {
    let val = match value.value? {
        Value::DoubleValue(val) => StatisticValue::Float64(val),
    };

    Some(val)
}

fn from_mangrobe_data_type(data_type: MangrobeDataType) -> anyhow::Result<ColumnDataType> {
    let data_type = data_type
        .r#type
        .context("Mangrobe API returned data_type without type")?;

    match data_type {
        data_type::Type::Scalar(value) => from_mangrobe_scalar_type(value),
        data_type::Type::Time(time) => Ok(ColumnDataType::Timestamp(from_mangrobe_time_unit(
            time.unit,
        )?)),
    }
}

fn from_mangrobe_scalar_type(value: i32) -> anyhow::Result<ColumnDataType> {
    match MangrobeScalarType::try_from(value)
        .context("Mangrobe API returned invalid scalar type")?
    {
        MangrobeScalarType::Bool => Ok(ColumnDataType::Bool),
        MangrobeScalarType::Int32 => Ok(ColumnDataType::Int32),
        MangrobeScalarType::Int64 => Ok(ColumnDataType::Int64),
        MangrobeScalarType::Float64 => Ok(ColumnDataType::Float64),
        MangrobeScalarType::String => Ok(ColumnDataType::String),
        MangrobeScalarType::Date => Ok(ColumnDataType::Date),
        other => Err(anyhow!(
            "Mangrobe API returned unsupported scalar type: {other:?}"
        )),
    }
}

fn from_mangrobe_time_unit(value: i32) -> anyhow::Result<TimeUnit> {
    match MangrobeTimeUnit::try_from(value).context("Mangrobe API returned invalid time unit")? {
        MangrobeTimeUnit::Second => Ok(TimeUnit::Second),
        MangrobeTimeUnit::Millisecond => Ok(TimeUnit::Millisecond),
        MangrobeTimeUnit::Microsecond => Ok(TimeUnit::Microsecond),
        MangrobeTimeUnit::Nanosecond => Ok(TimeUnit::Nanosecond),
        other => Err(anyhow!(
            "Mangrobe API returned unsupported time unit: {other:?}"
        )),
    }
}

fn to_mangrobe_table_definition(table: TableSchema) -> MangrobeTableDefinition {
    let stream_field = to_mangrobe_stream_field(table.stream_column());
    let partition_field = to_mangrobe_partition_field(
        table.partition_column(),
        table.partition_data_type().clone(),
    );

    MangrobeTableDefinition {
        identifier: Some(to_mangrobe_table_identifier(&table.table_name)),
        location: Some(to_mangrobe_external_location(table.location)),
        format: to_mangrobe_file_format(table.format) as i32,
        columns: table
            .public_columns
            .into_iter()
            .map(to_mangrobe_column)
            .collect(),
        stream_field: Some(stream_field),
        partition_field: Some(partition_field),
        comment: table.comment,
    }
}

fn to_mangrobe_external_location(location: ExternalLocation) -> MangrobeExternalLocation {
    MangrobeExternalLocation {
        storage_scheme: MangrobeStorageScheme::S3 as i32,
        bucket: Some(location.bucket),
        prefix: Some(location.prefix),
        endpoint: location.endpoint,
        region: location.region,
    }
}

fn to_mangrobe_column(column: PublicColumnDefinition) -> MangrobeColumn {
    MangrobeColumn {
        name: column.name,
        data_type: Some(to_mangrobe_data_type(column.data_type)),
        nullable: column.nullable,
        comment: column.comment,
    }
}

fn to_mangrobe_stream_field(column_name: String) -> MangrobeStreamField {
    MangrobeStreamField {
        src_column: column_name,
        dst_column: None,
        transform: MangrobePartitionTransform::Identity as i32,
        result_type: MangrobeStreamDataType::Int64 as i32,
    }
}

fn to_mangrobe_partition_field(
    column: String,
    data_type: ColumnDataType,
) -> MangrobePartitionField {
    MangrobePartitionField {
        src_column: column,
        dst_column: None,
        transform: MangrobePartitionTransform::Identity as i32,
        result_type: to_mangrobe_partition_data_type(data_type) as i32,
    }
}

fn to_mangrobe_partition_data_type(data_type: ColumnDataType) -> MangrobePartitionDataType {
    match data_type {
        ColumnDataType::Timestamp(TimeUnit::Microsecond) => {
            MangrobePartitionDataType::TimeMicrosecond
        }
        ColumnDataType::Int64 => MangrobePartitionDataType::Int64,
        other => unreachable!("unsupported partition data type: {other}"),
    }
}

fn to_mangrobe_file_format(format: FileFormat) -> MangrobeFileFormat {
    match format {
        FileFormat::Vortex => MangrobeFileFormat::Vortex,
    }
}

fn to_mangrobe_data_type(data_type: ColumnDataType) -> MangrobeDataType {
    MangrobeDataType {
        r#type: Some(match data_type {
            ColumnDataType::Bool => data_type::Type::Scalar(MangrobeScalarType::Bool as i32),
            ColumnDataType::Int32 => data_type::Type::Scalar(MangrobeScalarType::Int32 as i32),
            ColumnDataType::Int64 => data_type::Type::Scalar(MangrobeScalarType::Int64 as i32),
            ColumnDataType::Float64 => data_type::Type::Scalar(MangrobeScalarType::Float64 as i32),
            ColumnDataType::String => data_type::Type::Scalar(MangrobeScalarType::String as i32),
            ColumnDataType::Date => data_type::Type::Scalar(MangrobeScalarType::Date as i32),
            ColumnDataType::Timestamp(unit) => data_type::Type::Time(MangrobeTimestampType {
                unit: to_mangrobe_time_unit(unit) as i32,
            }),
        }),
    }
}

fn to_mangrobe_time_unit(unit: TimeUnit) -> MangrobeTimeUnit {
    match unit {
        TimeUnit::Second => MangrobeTimeUnit::Second,
        TimeUnit::Millisecond => MangrobeTimeUnit::Millisecond,
        TimeUnit::Microsecond => MangrobeTimeUnit::Microsecond,
        TimeUnit::Nanosecond => MangrobeTimeUnit::Nanosecond,
    }
}

fn to_mangrobe_partition_filter(filter: &PartitionFilter) -> MangrobePartitionFilter {
    MangrobePartitionFilter {
        predicates: filter
            .predicates
            .iter()
            .map(to_mangrobe_partition_predicate)
            .collect(),
    }
}

fn to_mangrobe_partition_predicate(predicate: &PartitionPredicate) -> MangrobePartitionPredicate {
    match predicate {
        PartitionPredicate::In(partitions) => MangrobePartitionPredicate {
            predicate: Some(partition_predicate::Predicate::In(PartitionIn {
                partitions: partitions
                    .iter()
                    .copied()
                    .map(to_mangrobe_partition_value)
                    .collect(),
            })),
        },
        PartitionPredicate::Range(range) => MangrobePartitionPredicate {
            predicate: Some(partition_predicate::Predicate::Range(PartitionRange {
                lower: range.lower.as_ref().map(to_mangrobe_partition_bound),
                upper: range.upper.as_ref().map(to_mangrobe_partition_bound),
            })),
        },
    }
}

fn to_mangrobe_partition_bound(bound: &PartitionRangeBound) -> MangrobePartitionBound {
    MangrobePartitionBound {
        partition: Some(to_mangrobe_partition_value(bound.partition)),
        inclusivity: match bound.inclusivity {
            BoundInclusivity::Inclusive => MangrobeBoundInclusivity::Inclusive,
            BoundInclusivity::Exclusive => MangrobeBoundInclusivity::Exclusive,
        } as i32,
    }
}

fn to_mangrobe_partition_value(partition: Partition) -> PartitionValue {
    PartitionValue {
        value: Some(match partition {
            Partition::TimeMicrosecond(value) => {
                partition_value::Value::Time(timestamp_from_micros(value))
            }
            Partition::Int64(value) => partition_value::Value::Int64Value(value),
        }),
    }
}

fn from_mangrobe_partition_value(value: &PartitionValue) -> anyhow::Result<Partition> {
    match value
        .value
        .as_ref()
        .context("Mangrobe API returned partition without value")?
    {
        partition_value::Value::Time(value) => {
            Ok(Partition::TimeMicrosecond(micros_from_timestamp(value)))
        }
        partition_value::Value::Int64Value(value) => Ok(Partition::Int64(*value)),
    }
}

fn to_mangrobe_metadata_type(value: FileMetadataType) -> i32 {
    (match value {
        FileMetadataType::ParquetMetadata => MangrobeFileMetadataType::ParquetMetadata,
    }) as i32
}

fn to_mangrobe_column_statistics_type(value: FileColumnStatisticsType) -> i32 {
    (match value {
        FileColumnStatisticsType::Min => MangrobeFileColumnStatisticsType::Min,
        FileColumnStatisticsType::Max => MangrobeFileColumnStatisticsType::Max,
    }) as i32
}

fn to_mangrobe_add_file_entry(entry: AddFilesEntry) -> anyhow::Result<MangrobeAddFileEntry> {
    Ok(MangrobeAddFileEntry {
        partition: Some(to_mangrobe_partition_value(entry.partition)),
        file_info_entries: entry
            .files
            .into_iter()
            .map(to_mangrobe_add_file_info_entry)
            .collect::<anyhow::Result<Vec<_>>>()?,
    })
}

fn to_mangrobe_add_file_info_entry(
    file: crate::domain::port::catalog::AddFile,
) -> anyhow::Result<MangrobeAddFileInfoEntry> {
    Ok(MangrobeAddFileInfoEntry {
        path: file.path,
        size: i64::try_from(file.size).context("file size does not fit in i64")?,
        column_statistics: file
            .column_statistics
            .columns
            .into_iter()
            .map(|statistics| MangrobeColumnStatisticsEntry {
                column_name: statistics.column_name,
                min: statistics.min.map(statistic_value_to_f64),
                max: statistics.max.map(statistic_value_to_f64),
            })
            .collect(),
        file_metadata: Some(MangrobeFileMetadataEntry {
            parquet_metadata: file.file_metadata.parquet_metadata,
        }),
    })
}

fn statistic_value_to_f64(value: StatisticValue) -> f64 {
    match value {
        StatisticValue::Int32(value) => value as f64,
        StatisticValue::Int64(value) => value as f64,
        StatisticValue::Float64(value) => value,
        StatisticValue::TimestampMicros(value) => value as f64,
    }
}

pub fn timestamp_from_micros(micros: i64) -> Timestamp {
    Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: (micros.rem_euclid(1_000_000) * 1_000) as i32,
    }
}

fn micros_from_timestamp(timestamp: &Timestamp) -> i64 {
    timestamp.seconds * 1_000_000 + i64::from(timestamp.nanos) / 1_000
}
