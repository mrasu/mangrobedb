use crate::domain::port::catalog::{
    AddFilesEntry, BoundInclusivity, CatalogError, CatalogFile, CatalogFileInfo, CatalogPort,
    FileColumnStatisticsType, FileMetadataType, PartitionTimeBound, PartitionTimeFilter,
    PartitionTimePredicate,
};
use crate::domain::statistics::{ColumnStatistics, StatisticValue};
use crate::domain::table_schema::{DUMMY_TABLE, TableSchema, initial_dummy_table_schema};
use crate::infrastructure::catalog::mock::{MockState, MockTable};
use crate::infrastructure::catalog::persisted::PersistedState;
use anyhow::{Context, anyhow};
use async_trait::async_trait;
use mangrobe_api_server::Mangrobe;
use mangrobe_api_server::proto::{
    AddFileEntry as MangrobeAddFileEntry, AddFileInfoEntry as MangrobeAddFileInfoEntry,
    AddFilesRequest, BoundInclusivity as MangrobeBoundInclusivity,
    ColumnStatisticsEntry as MangrobeColumnStatisticsEntry,
    FileColumnStatisticsType as MangrobeFileColumnStatisticsType,
    FileMetadataEntry as MangrobeFileMetadataEntry, FileMetadataType as MangrobeFileMetadataType,
    GetCurrentStateRequest, GetFileInfoRequest, IdempotencyKey,
    PartitionTimeBound as MangrobePartitionTimeBound,
    PartitionTimeFilter as MangrobePartitionTimeFilter, PartitionTimeIn,
    PartitionTimePredicate as MangrobePartitionTimePredicate, PartitionTimeRange,
    partition_time_predicate,
};
use prost_types::Timestamp;
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::debug;

const DEFAULT_HALF_MOCKED_STATE_PATH: &str = "./data/mock/half_mocked_state.json";

pub struct MangrobeCatalog {
    mangrobe: Mangrobe,
    state_path: PathBuf,
    state: Mutex<MockState>,
}

impl MangrobeCatalog {
    pub fn load_default(db: DatabaseConnection) -> anyhow::Result<Self> {
        Self::load(db, DEFAULT_HALF_MOCKED_STATE_PATH)
    }

    pub fn load(db: DatabaseConnection, state_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let state_path = state_path.into();
        debug!(
            state_path = %state_path.display(),
            "loading half-mocked mangrobe catalog port"
        );
        let state = if state_path.exists() {
            let json = fs::read_to_string(&state_path).with_context(|| {
                format!(
                    "failed to read half-mocked mangrobe state: {}",
                    state_path.display()
                )
            })?;
            serde_json::from_str::<PersistedState>(&json)
                .with_context(|| {
                    format!(
                        "failed to parse half-mocked mangrobe state: {}",
                        state_path.display()
                    )
                })?
                .try_into_state()?
        } else {
            initial_schema_state()
        };

        Ok(Self {
            mangrobe: Mangrobe::new_with_connection(db),
            state_path,
            state: Mutex::new(state),
        })
    }

    fn save(&self, state: &MockState) -> anyhow::Result<()> {
        debug!(
            state_path = %self.state_path.display(),
            "saving half-mocked mangrobe catalog schema state"
        );
        if let Some(parent) = self.state_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create half-mocked mangrobe state dir: {}",
                    parent.display()
                )
            })?;
        }

        let schema_only_state = schema_only_state(state);
        let json =
            serde_json::to_string_pretty(&PersistedState::try_from_state(&schema_only_state)?)
                .context("failed to serialize half-mocked mangrobe state")?;
        fs::write(&self.state_path, json).with_context(|| {
            format!(
                "failed to write half-mocked mangrobe state: {}",
                self.state_path.display()
            )
        })?;
        Ok(())
    }

    pub fn save_current_state(&self) -> anyhow::Result<()> {
        debug!("saving current half-mocked mangrobe catalog schema state");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("half-mocked mangrobe catalog state lock is poisoned"))?;

        self.save(&state)
    }
}

impl fmt::Debug for MangrobeCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MangrobeCatalog")
            .field("state_path", &self.state_path)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CatalogPort for MangrobeCatalog {
    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogError> {
        debug!(
            table_name,
            "getting table schema from half-mocked mangrobe catalog port"
        );
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("half-mocked mangrobe catalog state lock is poisoned"))?;

        state
            .tables
            .get(table_name)
            .map(|table| table.schema.clone())
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            })
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
            table_name: table_name.into(),
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
            table_name: table_name.into(),
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

    async fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError> {
        debug!(
            table_name,
            "updating table schema in half-mocked mangrobe catalog port"
        );
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("half-mocked mangrobe catalog state lock is poisoned"))?;

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
            table_name: table_name.into(),
            stream_id,
            add_file_entries,
            ..Default::default()
        };

        self.mangrobe.data_manipulation().add_files(param).await?;

        Ok(())
    }
}

fn initial_schema_state() -> MockState {
    let table = MockTable {
        name: DUMMY_TABLE.to_string(),
        schema: initial_dummy_table_schema(),
        files: Vec::new(),
    };
    let mut tables = HashMap::new();
    tables.insert(table.name.clone(), table);

    MockState { tables }
}

fn schema_only_state(state: &MockState) -> MockState {
    MockState {
        tables: state
            .tables
            .iter()
            .map(|(name, table)| {
                (
                    name.clone(),
                    MockTable {
                        name: table.name.clone(),
                        schema: table.schema.clone(),
                        files: Vec::new(),
                    },
                )
            })
            .collect(),
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
