use crate::domain::port::catalog::{
    AddFilesEntry, BoundInclusivity, CatalogError, CatalogFile, CatalogFileInfo, CatalogPort,
    ColumnDataType as CatalogColumnDataType,
    CreateExternalTableRequest as CatalogCreateExternalTableRequest,
    ExternalLocation as CatalogExternalLocation,
    ExternalTableDefinition as CatalogExternalTableDefinition, FileColumnStatisticsType,
    FileFormat as CatalogFileFormat, FileMetadataType, PartitionField as CatalogPartitionField,
    PartitionTimeBound, PartitionTimeFilter, PartitionTimePredicate,
    PartitionTransform as CatalogPartitionTransform, TableColumn as CatalogTableColumn,
    TableSummary as CatalogTableSummary, TimeUnit as CatalogTimeUnit,
};
use crate::domain::statistics::{ColumnStatistics, StatisticValue};
use crate::domain::table_schema::TableSchema;
use anyhow::{Context, anyhow};
use async_trait::async_trait;
use mangrobe_api_server::Mangrobe;
use mangrobe_api_server::proto::{
    AddFileEntry as MangrobeAddFileEntry, AddFileInfoEntry as MangrobeAddFileInfoEntry,
    AddFilesRequest, BoundInclusivity as MangrobeBoundInclusivity, Column as MangrobeColumn,
    ColumnStatisticsEntry as MangrobeColumnStatisticsEntry,
    CreateExternalTableRequest as MangrobeCreateExternalTableRequest, DataType as MangrobeDataType,
    EvolveTableSchemaRequest, ExternalLocation as MangrobeExternalLocation,
    FileColumnStatisticsType as MangrobeFileColumnStatisticsType, FileFormat as MangrobeFileFormat,
    FileMetadataEntry as MangrobeFileMetadataEntry, FileMetadataType as MangrobeFileMetadataType,
    GetCurrentStateRequest, GetFileInfoRequest, GetTableRequest as MangrobeGetTableRequest,
    IdempotencyKey, ListTablesRequest as MangrobeListTablesRequest,
    PartitionField as MangrobePartitionField, PartitionTimeBound as MangrobePartitionTimeBound,
    PartitionTimeFilter as MangrobePartitionTimeFilter, PartitionTimeIn,
    PartitionTimePredicate as MangrobePartitionTimePredicate, PartitionTimeRange,
    PartitionTransform as MangrobePartitionTransform, ScalarType as MangrobeScalarType,
    StorageScheme as MangrobeStorageScheme, TableDefinition as MangrobeTableDefinition,
    TableIdentifier, TimeType as MangrobeTimeType, TimeUnit as MangrobeTimeUnit, data_type,
    partition_time_predicate,
};
use prost_types::Timestamp;
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use std::fmt;

pub const MANGROBE_DB_CATALOG_NAME: &str = "mangrobe_db";
pub const MANGROBE_DB_SCHEMA_NAME: &str = "default";

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
    #[allow(
        clippy::needless_update,
        reason = "Keep the default update so this remains valid when the type is extended."
    )]
    async fn create_external_table(
        &self,
        request: CatalogCreateExternalTableRequest,
    ) -> Result<(), CatalogError> {
        let param = MangrobeCreateExternalTableRequest {
            table: Some(to_mangrobe_table_definition(request.table)),
            skip_if_exists: request.skip_if_exists,
            ..Default::default()
        };

        self.mangrobe
            .data_definition()
            .create_external_table(param)
            .await?;

        Ok(())
    }

    #[allow(
        clippy::needless_update,
        reason = "Keep the default update so this remains valid when the type is extended."
    )]
    async fn list_tables(&self) -> Result<Vec<CatalogTableSummary>, CatalogError> {
        let param = MangrobeListTablesRequest {
            catalog_name: Some(MANGROBE_DB_CATALOG_NAME.into()),
            schema_name: Some(MANGROBE_DB_SCHEMA_NAME.into()),
            ..Default::default()
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

    #[allow(
        clippy::needless_update,
        reason = "Keep the default update so this remains valid when the type is extended."
    )]
    async fn get_table(
        &self,
        table_name: &str,
    ) -> Result<CatalogExternalTableDefinition, CatalogError> {
        let param = MangrobeGetTableRequest {
            identifier: Some(to_mangrobe_table_identifier(table_name)),
            ..Default::default()
        };
        let response = self.mangrobe.data_definition().get_table(param).await?;
        let table = response
            .table
            .context("Mangrobe API returned get_table response without table")?;

        from_mangrobe_table_definition(table).map_err(CatalogError::from)
    }

    #[allow(
        clippy::needless_update,
        reason = "Keep the default update so this remains valid when the type is extended."
    )]
    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogError> {
        let param = MangrobeGetTableRequest {
            identifier: Some(to_mangrobe_table_identifier(table_name)),
            ..Default::default()
        };
        let response = self.mangrobe.data_definition().get_table(param).await?;
        let table = response
            .table
            .context("Mangrobe API returned get_table response without table")?;

        let table_schema = from_mangrobe_table_definition(table)
            .map_err(CatalogError::from)?
            .table_scheme();
        Ok(table_schema)
    }

    #[allow(
        clippy::needless_update,
        reason = "Keep the default update so this remains valid when the type is extended."
    )]
    async fn get_current_state(
        &self,
        table_name: &str,
        stream_id: i64,
        partition_time_filter: &PartitionTimeFilter,
    ) -> Result<Vec<CatalogFile>, CatalogError> {
        let param = GetCurrentStateRequest {
            table_identifier: Some(to_mangrobe_table_identifier(table_name)),
            stream_id,
            partition_time_filter: Some(to_mangrobe_partition_time_filter(partition_time_filter)),
            ..Default::default()
        };
        let response = self
            .mangrobe
            .data_manipulation()
            .get_current_state(param)
            .await?;

        let mut files = Vec::new();
        for partition in response.partitions {
            let partition_time = micros_from_timestamp(
                partition
                    .partition_time
                    .as_ref()
                    .context("Mangrobe API returned partition without partition_time")?,
            );
            for file in partition.files {
                files.push(CatalogFile {
                    file_id: file.file_id,
                    partition_time,
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
    ) -> Result<HashMap<String, CatalogFileInfo>, CatalogError> {
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
                .map(to_mangrobe_statistics_type)
                .collect(),
            included_file_metadata_types: included_file_metadata_types
                .iter()
                .copied()
                .map(to_mangrobe_metadata_type)
                .collect(),
            ..Default::default()
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
                    CatalogFileInfo {
                        file_id,
                        path: file.path,
                        size: u64::try_from(file.size)
                            .context("Mangrobe API returned negative file size")?,
                        column_statistics: file
                            .column_statistics
                            .into_iter()
                            .map(|statistics| ColumnStatistics {
                                column_name: statistics.column_name,
                                min: statistics
                                    .min
                                    .and_then(StatisticValue::from_statistics_value),
                                max: statistics
                                    .max
                                    .and_then(StatisticValue::from_statistics_value),
                            })
                            .collect(),
                        file_metadata: crate::domain::port::catalog::FileMetadata {
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

    #[allow(
        clippy::needless_update,
        reason = "Keep the default update so this remains valid when the type is extended."
    )]
    async fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError> {
        let proposed_columns = schema
            .public_columns()
            .iter()
            .map(|column| {
                Ok(to_mangrobe_column(CatalogTableColumn {
                    name: column.name.clone(),
                    data_type: CatalogColumnDataType::try_from(column.data_type().clone())?,
                    nullable: true,
                    comment: None,
                }))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let param = EvolveTableSchemaRequest {
            identifier: Some(to_mangrobe_table_identifier(table_name)),
            proposed_columns,
            ..Default::default()
        };

        self.mangrobe
            .data_definition()
            .evolve_table_schema(param)
            .await?;

        Ok(())
    }

    #[allow(
        clippy::needless_update,
        reason = "Keep the default update so this remains valid when the type is extended."
    )]
    async fn add_files(
        &self,
        idempotency_key: &[u8],
        table_name: &str,
        stream_id: i64,
        entries: Vec<AddFilesEntry>,
    ) -> Result<(), CatalogError> {
        let add_file_entries = entries
            .into_iter()
            .map(to_mangrobe_add_file_entry)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let param = AddFilesRequest {
            idempotency_key: Some(IdempotencyKey {
                key: idempotency_key.to_vec(),
                ..Default::default()
            }),
            table_identifier: Some(to_mangrobe_table_identifier(table_name)),
            stream_id,
            add_file_entries,
            ..Default::default()
        };

        self.mangrobe.data_manipulation().add_files(param).await?;

        Ok(())
    }
}

fn to_mangrobe_table_identifier(table_name: &str) -> TableIdentifier {
    TableIdentifier {
        catalog_name: MANGROBE_DB_CATALOG_NAME.into(),
        schema_name: MANGROBE_DB_SCHEMA_NAME.into(),
        table_name: table_name.into(),
    }
}

fn from_mangrobe_table_definition(
    table: MangrobeTableDefinition,
) -> anyhow::Result<CatalogExternalTableDefinition> {
    let identifier = table
        .identifier
        .context("Mangrobe API returned table without identifier")?;
    Ok(CatalogExternalTableDefinition {
        table_name: identifier.table_name,
        location: from_mangrobe_external_location(
            table
                .location
                .context("Mangrobe API returned table without location")?,
        )?,
        format: from_mangrobe_file_format(table.format)?,
        columns: table
            .columns
            .into_iter()
            .map(from_mangrobe_column)
            .collect::<anyhow::Result<Vec<_>>>()?,
        partition_fields: table
            .partition_fields
            .into_iter()
            .map(from_mangrobe_partition_field)
            .collect::<anyhow::Result<Vec<_>>>()?,
        comment: table.comment,
    })
}

fn from_mangrobe_external_location(
    location: MangrobeExternalLocation,
) -> anyhow::Result<CatalogExternalLocation> {
    let storage_scheme = MangrobeStorageScheme::try_from(location.storage_scheme)
        .context("Mangrobe API returned invalid storage scheme")?;
    if storage_scheme != MangrobeStorageScheme::S3 {
        return Err(anyhow!(
            "Mangrobe API returned unsupported storage scheme: {storage_scheme:?}"
        ));
    }

    Ok(CatalogExternalLocation {
        bucket: location
            .bucket
            .context("Mangrobe API returned S3 location without bucket")?,
        prefix: location.prefix.unwrap_or_default(),
        endpoint: location.endpoint,
        region: location.region,
    })
}

fn from_mangrobe_column(column: MangrobeColumn) -> anyhow::Result<CatalogTableColumn> {
    Ok(CatalogTableColumn {
        name: column.name,
        data_type: from_mangrobe_data_type(
            column
                .data_type
                .context("Mangrobe API returned column without data_type")?,
        )?,
        nullable: column.nullable,
        comment: column.comment,
    })
}

fn from_mangrobe_partition_field(
    field: MangrobePartitionField,
) -> anyhow::Result<CatalogPartitionField> {
    Ok(CatalogPartitionField {
        source_column: field.src_column,
        destination_column: field.dst_column,
        transform: from_mangrobe_partition_transform(field.transform)?,
        result_type: from_mangrobe_data_type(
            field
                .result_type
                .context("Mangrobe API returned partition field without result_type")?,
        )?,
    })
}

fn from_mangrobe_file_format(value: i32) -> anyhow::Result<CatalogFileFormat> {
    match MangrobeFileFormat::try_from(value)
        .context("Mangrobe API returned invalid file format")?
    {
        MangrobeFileFormat::Vortex => Ok(CatalogFileFormat::Vortex),
        other => Err(anyhow!(
            "Mangrobe API returned unsupported file format: {other:?}"
        )),
    }
}

fn from_mangrobe_partition_transform(value: i32) -> anyhow::Result<CatalogPartitionTransform> {
    match MangrobePartitionTransform::try_from(value)
        .context("Mangrobe API returned invalid partition transform")?
    {
        MangrobePartitionTransform::Identity => Ok(CatalogPartitionTransform::Identity),
        other => Err(anyhow!(
            "Mangrobe API returned unsupported partition transform: {other:?}"
        )),
    }
}

fn from_mangrobe_data_type(data_type: MangrobeDataType) -> anyhow::Result<CatalogColumnDataType> {
    let data_type = data_type
        .r#type
        .context("Mangrobe API returned data_type without type")?;

    match data_type {
        data_type::Type::Scalar(value) => from_mangrobe_scalar_type(value),
        data_type::Type::Time(time) => Ok(CatalogColumnDataType::Time(from_mangrobe_time_unit(
            time.unit,
        )?)),
    }
}

fn from_mangrobe_scalar_type(value: i32) -> anyhow::Result<CatalogColumnDataType> {
    match MangrobeScalarType::try_from(value)
        .context("Mangrobe API returned invalid scalar type")?
    {
        MangrobeScalarType::Bool => Ok(CatalogColumnDataType::Bool),
        MangrobeScalarType::Int32 => Ok(CatalogColumnDataType::Int32),
        MangrobeScalarType::Int64 => Ok(CatalogColumnDataType::Int64),
        MangrobeScalarType::Float64 => Ok(CatalogColumnDataType::Float64),
        MangrobeScalarType::String => Ok(CatalogColumnDataType::String),
        MangrobeScalarType::Date => Ok(CatalogColumnDataType::Date),
        other => Err(anyhow!(
            "Mangrobe API returned unsupported scalar type: {other:?}"
        )),
    }
}

fn from_mangrobe_time_unit(value: i32) -> anyhow::Result<CatalogTimeUnit> {
    match MangrobeTimeUnit::try_from(value).context("Mangrobe API returned invalid time unit")? {
        MangrobeTimeUnit::Second => Ok(CatalogTimeUnit::Second),
        MangrobeTimeUnit::Millisecond => Ok(CatalogTimeUnit::Millisecond),
        MangrobeTimeUnit::Microsecond => Ok(CatalogTimeUnit::Microsecond),
        MangrobeTimeUnit::Nanosecond => Ok(CatalogTimeUnit::Nanosecond),
        other => Err(anyhow!(
            "Mangrobe API returned unsupported time unit: {other:?}"
        )),
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_table_definition(
    table: crate::domain::port::catalog::ExternalTableDefinition,
) -> MangrobeTableDefinition {
    MangrobeTableDefinition {
        identifier: Some(to_mangrobe_table_identifier(&table.table_name)),
        location: Some(to_mangrobe_external_location(table.location)),
        format: to_mangrobe_file_format(table.format) as i32,
        columns: table.columns.into_iter().map(to_mangrobe_column).collect(),
        partition_fields: table
            .partition_fields
            .into_iter()
            .map(to_mangrobe_partition_field)
            .collect(),
        comment: table.comment,
        ..Default::default()
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_external_location(
    location: crate::domain::port::catalog::ExternalLocation,
) -> MangrobeExternalLocation {
    MangrobeExternalLocation {
        storage_scheme: MangrobeStorageScheme::S3 as i32,
        bucket: Some(location.bucket),
        prefix: Some(location.prefix),
        endpoint: location.endpoint,
        region: location.region,
        ..Default::default()
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_column(column: CatalogTableColumn) -> MangrobeColumn {
    MangrobeColumn {
        name: column.name,
        data_type: Some(to_mangrobe_data_type(column.data_type)),
        nullable: column.nullable,
        comment: column.comment,
        ..Default::default()
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_partition_field(field: CatalogPartitionField) -> MangrobePartitionField {
    MangrobePartitionField {
        src_column: field.source_column,
        dst_column: field.destination_column,
        transform: to_mangrobe_partition_transform(field.transform) as i32,
        result_type: Some(to_mangrobe_data_type(field.result_type)),
        ..Default::default()
    }
}

fn to_mangrobe_file_format(format: CatalogFileFormat) -> MangrobeFileFormat {
    match format {
        CatalogFileFormat::Vortex => MangrobeFileFormat::Vortex,
    }
}

fn to_mangrobe_partition_transform(
    transform: CatalogPartitionTransform,
) -> MangrobePartitionTransform {
    match transform {
        CatalogPartitionTransform::Identity => MangrobePartitionTransform::Identity,
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_data_type(data_type: CatalogColumnDataType) -> MangrobeDataType {
    MangrobeDataType {
        r#type: Some(match data_type {
            CatalogColumnDataType::Bool => data_type::Type::Scalar(MangrobeScalarType::Bool as i32),
            CatalogColumnDataType::Int32 => {
                data_type::Type::Scalar(MangrobeScalarType::Int32 as i32)
            }
            CatalogColumnDataType::Int64 => {
                data_type::Type::Scalar(MangrobeScalarType::Int64 as i32)
            }
            CatalogColumnDataType::Float64 => {
                data_type::Type::Scalar(MangrobeScalarType::Float64 as i32)
            }
            CatalogColumnDataType::String => {
                data_type::Type::Scalar(MangrobeScalarType::String as i32)
            }
            CatalogColumnDataType::Date => data_type::Type::Scalar(MangrobeScalarType::Date as i32),
            CatalogColumnDataType::Time(unit) => data_type::Type::Time(MangrobeTimeType {
                unit: to_mangrobe_time_unit(unit) as i32,
                ..Default::default()
            }),
        }),
        ..Default::default()
    }
}

fn to_mangrobe_time_unit(unit: CatalogTimeUnit) -> MangrobeTimeUnit {
    match unit {
        CatalogTimeUnit::Second => MangrobeTimeUnit::Second,
        CatalogTimeUnit::Millisecond => MangrobeTimeUnit::Millisecond,
        CatalogTimeUnit::Microsecond => MangrobeTimeUnit::Microsecond,
        CatalogTimeUnit::Nanosecond => MangrobeTimeUnit::Nanosecond,
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_partition_time_filter(filter: &PartitionTimeFilter) -> MangrobePartitionTimeFilter {
    MangrobePartitionTimeFilter {
        predicates: filter
            .predicates
            .iter()
            .map(to_mangrobe_partition_time_predicate)
            .collect(),
        ..Default::default()
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_partition_time_predicate(
    predicate: &PartitionTimePredicate,
) -> MangrobePartitionTimePredicate {
    match predicate {
        PartitionTimePredicate::In(times) => MangrobePartitionTimePredicate {
            predicate: Some(partition_time_predicate::Predicate::In(PartitionTimeIn {
                times: times.iter().copied().map(timestamp_from_micros).collect(),
                ..Default::default()
            })),
            ..Default::default()
        },
        PartitionTimePredicate::Range(range) => MangrobePartitionTimePredicate {
            predicate: Some(partition_time_predicate::Predicate::Range(
                PartitionTimeRange {
                    lower: range.lower.as_ref().map(to_mangrobe_partition_time_bound),
                    upper: range.upper.as_ref().map(to_mangrobe_partition_time_bound),
                    ..Default::default()
                },
            )),
            ..Default::default()
        },
    }
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_partition_time_bound(bound: &PartitionTimeBound) -> MangrobePartitionTimeBound {
    MangrobePartitionTimeBound {
        time: Some(timestamp_from_micros(bound.time)),
        inclusivity: match bound.inclusivity {
            BoundInclusivity::Inclusive => MangrobeBoundInclusivity::Inclusive,
            BoundInclusivity::Exclusive => MangrobeBoundInclusivity::Exclusive,
        } as i32,
        ..Default::default()
    }
}

fn to_mangrobe_statistics_type(value: FileColumnStatisticsType) -> i32 {
    (match value {
        FileColumnStatisticsType::Min => MangrobeFileColumnStatisticsType::Min,
        FileColumnStatisticsType::Max => MangrobeFileColumnStatisticsType::Max,
    }) as i32
}

fn to_mangrobe_metadata_type(value: FileMetadataType) -> i32 {
    (match value {
        FileMetadataType::ParquetMetadata => MangrobeFileMetadataType::ParquetMetadata,
    }) as i32
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
fn to_mangrobe_add_file_entry(entry: AddFilesEntry) -> anyhow::Result<MangrobeAddFileEntry> {
    Ok(MangrobeAddFileEntry {
        partition_time: Some(timestamp_from_micros(entry.partition_time)),
        file_info_entries: entry
            .files
            .into_iter()
            .map(to_mangrobe_add_file_info_entry)
            .collect::<anyhow::Result<Vec<_>>>()?,
        ..Default::default()
    })
}

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
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
                ..Default::default()
            })
            .collect(),
        file_metadata: Some(MangrobeFileMetadataEntry {
            parquet_metadata: file.file_metadata.parquet_metadata,
            ..Default::default()
        }),
        ..Default::default()
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

#[allow(
    clippy::needless_update,
    reason = "Keep the default update so this remains valid when the type is extended."
)]
pub fn timestamp_from_micros(micros: i64) -> Timestamp {
    Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: (micros.rem_euclid(1_000_000) * 1_000) as i32,
        ..Default::default()
    }
}

fn micros_from_timestamp(timestamp: &Timestamp) -> i64 {
    timestamp.seconds * 1_000_000 + i64::from(timestamp.nanos) / 1_000
}
