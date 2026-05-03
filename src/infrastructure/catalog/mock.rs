use crate::domain::port::catalog::{AddFile, AddFilesEntry, CatalogPort, CatalogPortError};
use crate::domain::statistics::FileStatistics;
use crate::domain::table_schema::{DUMMY_TABLE, TableSchema, initial_dummy_table_schema};
use crate::infrastructure::catalog::persisted::PersistedState;
use anyhow::{Context, anyhow};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::debug;

const DEFAULT_STATE_PATH: &str = "./data/mock/state.json";

#[derive(Debug)]
pub struct MockCatalogPort {
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
    pub(super) stream_id: i32,
    pub(super) partition_time: i64,
    pub(super) path: String,
    pub(super) size: u64,
    pub(super) column_statistics: FileStatistics,
}

impl MockCatalogPort {
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

impl CatalogPort for MockCatalogPort {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogPortError> {
        debug!(table_name, "getting table schema from mock catalog port");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        state
            .tables
            .get(table_name)
            .map(|table| table.schema.clone())
            .ok_or_else(|| CatalogPortError::TableNotFound {
                table_name: table_name.to_string(),
            })
    }

    fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogPortError> {
        debug!(table_name, "updating table schema in mock catalog port");
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let table =
            state
                .tables
                .get_mut(table_name)
                .ok_or_else(|| CatalogPortError::TableNotFound {
                    table_name: table_name.to_string(),
                })?;

        table.schema = schema;
        self.save(&state)?;

        Ok(())
    }

    fn add_files(
        &self,
        table_name: &str,
        stream_id: i32,
        entries: Vec<AddFilesEntry>,
    ) -> Result<(), CatalogPortError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock catalog port state lock is poisoned"))?;

        let table =
            state
                .tables
                .get_mut(table_name)
                .ok_or_else(|| CatalogPortError::TableNotFound {
                    table_name: table_name.to_string(),
                })?;

        append_add_files(table, stream_id, entries);

        self.save(&state)?;
        Ok(())
    }
}

fn append_add_files(table: &mut MockTable, stream_id: i32, entries: Vec<AddFilesEntry>) {
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

fn build_mock_catalog_file(stream_id: i32, partition_time: i64, file: AddFile) -> MockCatalogFile {
    MockCatalogFile {
        stream_id,
        partition_time,
        path: file.path,
        size: file.size,
        column_statistics: file.column_statistics,
    }
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
