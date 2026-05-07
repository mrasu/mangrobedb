use crate::domain::port::catalog::{
    AddFile, AddFilesEntry, BoundInclusivity, CatalogError, CatalogFile, CatalogFileInfo,
    CatalogPort, FileColumnStatisticsType, FileMetadata, FileMetadataType, PartitionTimeBound,
    PartitionTimeFilter, PartitionTimePredicate, PartitionTimeRange,
};
use crate::domain::statistics::{ColumnStatistics, FileStatistics};
use crate::domain::table_schema::{DUMMY_TABLE, TableSchema, initial_dummy_table_schema};
use crate::infrastructure::catalog::persisted::PersistedState;
use anyhow::{Context, anyhow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::debug;

const DEFAULT_STATE_PATH: &str = "./data/mock/state.json";
const FILE_ID_PREFIX: &str = "id:";

#[derive(Debug)]
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
