use crate::application::datafusion::column::to_internal_column_name;
use crate::domain::port::catalog::{
    AddFile, AddFilesEntry, BoundInclusivity, CatalogError, CatalogFile, CatalogFileInfo,
    CatalogPort, ColumnDataType as CatalogColumnDataType,
    CreateExternalTableRequest as CatalogCreateExternalTableRequest,
    ExternalLocation as CatalogExternalLocation,
    ExternalTableDefinition as CatalogExternalTableDefinition, FileColumnStatisticsType,
    FileFormat as CatalogFileFormat, FileMetadata, FileMetadataType,
    PartitionField as CatalogPartitionField, PartitionTimeBound, PartitionTimeFilter,
    PartitionTimePredicate, PartitionTimeRange, PartitionTransform as CatalogPartitionTransform,
    TableColumn as CatalogTableColumn, TableSummary as CatalogTableSummary,
    TimeUnit as CatalogTimeUnit,
};
use crate::domain::statistics::{ColumnStatistics, FileStatistics};
use crate::domain::table_mapping::{MappingStrategy, TableMapping};
use crate::domain::table_schema::{
    DUMMY_TABLE, InternalColumnDefinition, PublicColumnDefinition, TableSchema,
    initial_dummy_table_schema,
};
use crate::infrastructure::catalog::persisted::PersistedState;
use anyhow::{Context, anyhow};
use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::debug;

#[allow(dead_code)]
const DEFAULT_STATE_PATH: &str = "./data/mock/state.json";
const FILE_ID_PREFIX: &str = "id:";

#[allow(dead_code)]
#[derive(Debug)]
// TODO: remove when mangrobe supports table creation
pub struct MockCatalog {
    state_path: PathBuf,
    state: Mutex<MockState>,
}

#[derive(Debug)]
pub(super) struct MockState {
    pub(super) tables: HashMap<String, MockTable>,
}

#[derive(Debug)]
pub(super) struct MockTable {
    pub(super) name: String,
    pub(super) schema: TableSchema,
    pub(super) files: Vec<MockCatalogFile>,
}

#[derive(Debug, Clone)]
pub(super) struct MockCatalogFile {
    pub(super) stream_id: i64,
    pub(super) partition_time: i64,
    pub(super) path: String,
    pub(super) size: u64,
    pub(super) column_statistics: FileStatistics,
    pub(super) file_metadata: FileMetadata,
}

#[allow(dead_code)]
impl MockCatalog {
    pub fn load_default() -> anyhow::Result<Self> {
        Self::load(DEFAULT_STATE_PATH)
    }

    pub fn load(state_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let state_path = state_path.into();
        debug!(state_path = %state_path.display(), "loading mock catalog port");
        let state = if state_path.exists() {
            let json = fs::read_to_string(&state_path)
                .with_context(|| format!("failed to read mock state: {}", state_path.display()))?;
            serde_json::from_str::<PersistedState>(&json)
                .with_context(|| format!("failed to parse mock state: {}", state_path.display()))?
                .try_into_state()?
        } else {
            MockState::initial()
        };

        Ok(Self {
            state_path,
            state: Mutex::new(state),
        })
    }

    fn save(&self, state: &MockState) -> anyhow::Result<()> {
        debug!(state_path = %self.state_path.display(), "saving mock catalog port state");
        if let Some(parent) = self.state_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create mock state dir: {}", parent.display())
            })?;
        }

        let json = serde_json::to_string_pretty(&PersistedState::try_from_state(state)?)
            .context("failed to serialize mock state")?;
        fs::write(&self.state_path, json).with_context(|| {
            format!("failed to write mock state: {}", self.state_path.display())
        })?;
        Ok(())
    }

    pub fn save_current_state(&self) -> anyhow::Result<()> {
        debug!("saving current mock catalog port state");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        self.save(&state)
    }
}

#[async_trait]
impl CatalogPort for MockCatalog {
    async fn create_external_table(
        &self,
        request: CatalogCreateExternalTableRequest,
    ) -> Result<(), CatalogError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;
        let table_name = request.table.table_name.clone();

        if state.tables.contains_key(&table_name) {
            if request.skip_if_exists {
                return Ok(());
            }

            return Err(CatalogError::Internal(anyhow!(
                "table already exists: {table_name}"
            )));
        }

        let table = MockTable {
            name: table_name.clone(),
            schema: to_table_schema(request.table)?,
            files: Vec::new(),
        };
        state.tables.insert(table_name, table);

        self.save(&state)?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<CatalogTableSummary>, CatalogError> {
        debug!("listing tables from mock catalog port");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let mut tables = state
            .tables
            .values()
            .map(|table| CatalogTableSummary {
                table_name: table.name.clone(),
                comment: None,
            })
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.table_name.cmp(&right.table_name));

        Ok(tables)
    }

    async fn get_table(
        &self,
        table_name: &str,
    ) -> Result<CatalogExternalTableDefinition, CatalogError> {
        debug!(table_name, "getting table from mock catalog port");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let table = state
            .tables
            .get(table_name)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            })?;

        to_external_table_definition(&table.schema).map_err(CatalogError::from)
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogError> {
        debug!(table_name, "getting table schema from mock catalog port");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        state
            .tables
            .get(table_name)
            .map(|table| table.schema.clone())
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            })
    }

    async fn get_current_state(
        &self,
        table_name: &str,
        stream_id: i64,
        partition_time_filter: &PartitionTimeFilter,
    ) -> Result<Vec<CatalogFile>, CatalogError> {
        debug!(
            table_name,
            stream_id,
            ?partition_time_filter,
            "getting current state from mock catalog port"
        );
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let table = state
            .tables
            .get(table_name)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            })?;

        let files = table
            .files
            .iter()
            .filter(|file| file.stream_id == stream_id)
            .filter(|file| partition_time_matches(file.partition_time, partition_time_filter))
            .cloned()
            .map(Into::into)
            .collect();

        Ok(files)
    }

    async fn get_file_info(
        &self,
        table_name: &str,
        file_ids: &[String],
        included_column_statistics_types: &[FileColumnStatisticsType],
        included_file_metadata_types: &[FileMetadataType],
    ) -> Result<HashMap<String, CatalogFileInfo>, CatalogError> {
        debug!(
            table_name,
            ?file_ids,
            "getting file info from mock catalog port"
        );
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let table = state
            .tables
            .get(table_name)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            })?;
        let paths = file_ids
            .iter()
            .filter_map(|file_id| file_id_to_path(file_id))
            .collect::<Vec<_>>();

        let file_info = table
            .files
            .iter()
            .filter(|file| paths.contains(&file.path.as_str()))
            .map(|file| {
                let file_id = build_file_id(&file.path);
                (
                    file_id.clone(),
                    CatalogFileInfo {
                        file_id,
                        path: file.path.clone(),
                        size: file.size,
                        column_statistics: filter_column_statistics(
                            &file.column_statistics.columns,
                            included_column_statistics_types,
                        ),
                        file_metadata: filter_file_metadata(
                            &file.file_metadata,
                            included_file_metadata_types,
                        ),
                    },
                )
            })
            .collect();

        Ok(file_info)
    }

    async fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError> {
        debug!(table_name, "updating table schema in mock catalog port");
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let table =
            state
                .tables
                .get_mut(table_name)
                .ok_or_else(|| CatalogError::TableNotFound {
                    table_name: table_name.to_string(),
                })?;

        table.schema = schema;
        self.save(&state)?;

        Ok(())
    }

    async fn add_files(
        &self,
        _idempotency_key: &[u8],
        table_name: &str,
        stream_id: i64,
        entries: Vec<AddFilesEntry>,
    ) -> Result<(), CatalogError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let table =
            state
                .tables
                .get_mut(table_name)
                .ok_or_else(|| CatalogError::TableNotFound {
                    table_name: table_name.to_string(),
                })?;

        append_add_files(table, stream_id, entries);

        self.save(&state)?;
        Ok(())
    }
}

fn to_external_table_definition(
    schema: &TableSchema,
) -> anyhow::Result<CatalogExternalTableDefinition> {
    let columns = schema
        .public_columns()
        .iter()
        .map(|column| {
            Ok(CatalogTableColumn {
                name: column.name.clone(),
                data_type: from_arrow_data_type(column.data_type())?,
                nullable: true,
                comment: None,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let partition_column = schema.partition_time_mapping().src_column_ref();

    Ok(CatalogExternalTableDefinition {
        table_name: schema.table_name.clone(),
        location: CatalogExternalLocation {
            bucket: schema.bucket.clone(),
            prefix: schema.path_prefix.clone(),
            endpoint: None,
            region: None,
        },
        format: CatalogFileFormat::Vortex,
        columns,
        partition_fields: vec![CatalogPartitionField {
            source_column: partition_column.name.clone(),
            destination_column: None,
            transform: CatalogPartitionTransform::Identity,
            result_type: from_arrow_data_type(partition_column.data_type())?,
        }],
        comment: None,
    })
}

fn to_table_schema(
    table: crate::domain::port::catalog::ExternalTableDefinition,
) -> Result<TableSchema, CatalogError> {
    let stream_id_column = table
        .columns
        .iter()
        .find(|column| column.name == "stream_id")
        .ok_or_else(|| CatalogError::Internal(anyhow!("stream_id column is required")))?;
    let partition_field = table
        .partition_fields
        .first()
        .ok_or_else(|| CatalogError::Internal(anyhow!("partition field is required")))?;
    let partition_column = table
        .columns
        .iter()
        .find(|column| column.name == partition_field.source_column)
        .ok_or_else(|| {
            CatalogError::Internal(anyhow!(
                "partition source column not found: {}",
                partition_field.source_column
            ))
        })?;

    let public_columns = table
        .columns
        .iter()
        .cloned()
        .map(to_public_column_definition)
        .collect();
    let stream_id_mapping = TableMapping::new(
        to_public_column_definition(stream_id_column.clone()),
        InternalColumnDefinition::new(
            to_internal_column_name("stream_id"),
            to_arrow_data_type(&stream_id_column.data_type),
        ),
        MappingStrategy::Copy,
    );
    let partition_time_mapping = TableMapping::new(
        to_public_column_definition(partition_column.clone()),
        InternalColumnDefinition::new(
            to_internal_column_name("partition_time"),
            DataType::Timestamp(ArrowTimeUnit::Microsecond, None),
        ),
        MappingStrategy::ToHour,
    );

    Ok(TableSchema::new(
        table.table_name,
        table.location.bucket,
        table.location.prefix,
        public_columns,
        stream_id_mapping,
        partition_time_mapping,
    ))
}

fn to_public_column_definition(column: CatalogTableColumn) -> PublicColumnDefinition {
    PublicColumnDefinition::new(column.name, to_arrow_data_type(&column.data_type))
}

fn to_arrow_data_type(data_type: &CatalogColumnDataType) -> DataType {
    match data_type {
        CatalogColumnDataType::Bool => DataType::Boolean,
        CatalogColumnDataType::Int64 => DataType::Int64,
        CatalogColumnDataType::Float64 => DataType::Float64,
        CatalogColumnDataType::String => DataType::Utf8,
        CatalogColumnDataType::Date => DataType::Date32,
        CatalogColumnDataType::Time(unit) => DataType::Timestamp(to_arrow_time_unit(*unit), None),
    }
}

fn to_arrow_time_unit(unit: CatalogTimeUnit) -> ArrowTimeUnit {
    match unit {
        CatalogTimeUnit::Second => ArrowTimeUnit::Second,
        CatalogTimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
        CatalogTimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
        CatalogTimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
    }
}

fn from_arrow_data_type(data_type: &DataType) -> anyhow::Result<CatalogColumnDataType> {
    match data_type {
        DataType::Boolean => Ok(CatalogColumnDataType::Bool),
        DataType::Int32 | DataType::Int64 => Ok(CatalogColumnDataType::Int64),
        DataType::Float64 => Ok(CatalogColumnDataType::Float64),
        DataType::Utf8 => Ok(CatalogColumnDataType::String),
        DataType::Date32 => Ok(CatalogColumnDataType::Date),
        DataType::Timestamp(unit, _) => {
            Ok(CatalogColumnDataType::Time(from_arrow_time_unit(*unit)))
        }
        other => Err(anyhow!("unsupported mock catalog column type: {other:?}")),
    }
}

fn from_arrow_time_unit(unit: ArrowTimeUnit) -> CatalogTimeUnit {
    match unit {
        ArrowTimeUnit::Second => CatalogTimeUnit::Second,
        ArrowTimeUnit::Millisecond => CatalogTimeUnit::Millisecond,
        ArrowTimeUnit::Microsecond => CatalogTimeUnit::Microsecond,
        ArrowTimeUnit::Nanosecond => CatalogTimeUnit::Nanosecond,
    }
}

fn filter_column_statistics(
    columns: &[ColumnStatistics],
    included_types: &[FileColumnStatisticsType],
) -> Vec<ColumnStatistics> {
    let include_min = included_types.contains(&FileColumnStatisticsType::Min);
    let include_max = included_types.contains(&FileColumnStatisticsType::Max);

    columns
        .iter()
        .map(|column| ColumnStatistics {
            column_name: column.column_name.clone(),
            min: include_min.then(|| column.min.clone()).flatten(),
            max: include_max.then(|| column.max.clone()).flatten(),
        })
        .collect()
}

fn filter_file_metadata(
    metadata: &FileMetadata,
    included_types: &[FileMetadataType],
) -> FileMetadata {
    FileMetadata {
        parquet_metadata: included_types
            .contains(&FileMetadataType::ParquetMetadata)
            .then(|| metadata.parquet_metadata.clone())
            .flatten(),
    }
}

fn partition_time_matches(partition_time: i64, filter: &PartitionTimeFilter) -> bool {
    filter.predicates.is_empty()
        || filter
            .predicates
            .iter()
            .any(|predicate| partition_time_matches_predicate(partition_time, predicate))
}

fn partition_time_matches_predicate(
    partition_time: i64,
    predicate: &PartitionTimePredicate,
) -> bool {
    match predicate {
        PartitionTimePredicate::In(times) => times.contains(&partition_time),
        PartitionTimePredicate::Range(range) => partition_time_matches_range(partition_time, range),
    }
}

fn partition_time_matches_range(partition_time: i64, range: &PartitionTimeRange) -> bool {
    range
        .lower
        .as_ref()
        .is_none_or(|bound| lower_bound_matches(partition_time, bound))
        && range
            .upper
            .as_ref()
            .is_none_or(|bound| upper_bound_matches(partition_time, bound))
}

fn lower_bound_matches(partition_time: i64, bound: &PartitionTimeBound) -> bool {
    match bound.inclusivity {
        BoundInclusivity::Inclusive => partition_time >= bound.time,
        BoundInclusivity::Exclusive => partition_time > bound.time,
    }
}

fn upper_bound_matches(partition_time: i64, bound: &PartitionTimeBound) -> bool {
    match bound.inclusivity {
        BoundInclusivity::Inclusive => partition_time <= bound.time,
        BoundInclusivity::Exclusive => partition_time < bound.time,
    }
}

fn append_add_files(table: &mut MockTable, stream_id: i64, entries: Vec<AddFilesEntry>) {
    for entry in entries {
        for file in entry.files {
            table.files.push(build_mock_catalog_file(
                stream_id,
                entry.partition_time,
                file,
            ));
        }
    }

    table.files.sort_by(|left, right| {
        left.stream_id
            .cmp(&right.stream_id)
            .then(left.partition_time.cmp(&right.partition_time))
            .then(left.path.cmp(&right.path))
    });
}

fn build_mock_catalog_file(stream_id: i64, partition_time: i64, file: AddFile) -> MockCatalogFile {
    MockCatalogFile {
        stream_id,
        partition_time,
        path: file.path,
        size: file.size,
        column_statistics: file.column_statistics,
        file_metadata: file.file_metadata,
    }
}

impl From<MockCatalogFile> for CatalogFile {
    fn from(value: MockCatalogFile) -> Self {
        Self {
            file_id: build_file_id(&value.path),
            partition_time: value.partition_time,
            path: value.path,
            size: value.size,
        }
    }
}

fn build_file_id(path: &str) -> String {
    format!("{FILE_ID_PREFIX}{path}")
}

fn file_id_to_path(file_id: &str) -> Option<&str> {
    file_id.strip_prefix(FILE_ID_PREFIX)
}

#[allow(dead_code)]
impl MockState {
    fn initial() -> Self {
        let table = MockTable {
            name: DUMMY_TABLE.to_string(),
            schema: initial_dummy_table_schema(),
            files: Vec::new(),
        };
        let mut tables = HashMap::new();
        tables.insert(table.name.clone(), table);

        Self { tables }
    }
}
