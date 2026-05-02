use crate::domain::repository::{TableRepository, TableRepositoryError};
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
pub struct MockTableRepository {
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
}

impl MockTableRepository {
    pub fn load_default() -> anyhow::Result<Self> {
        Self::load(DEFAULT_STATE_PATH)
    }

    pub fn load(state_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let state_path = state_path.into();
        debug!(state_path = %state_path.display(), "loading mock table repository");
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
        debug!(state_path = %self.state_path.display(), "saving mock table repository state");
        if let Some(parent) = self.state_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create mock state dir: {}", parent.display()))?;
        }

        let json = serde_json::to_string_pretty(&PersistedState::try_from_state(state)?)
            .context("failed to serialize mock state")?;
        fs::write(&self.state_path, json)
            .with_context(|| format!("failed to write mock state: {}", self.state_path.display()))?;
        Ok(())
    }

    pub fn save_current_state(&self) -> anyhow::Result<()> {
        debug!("saving current mock table repository state");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock table repository state lock is poisoned"))?;

        self.save(&state)
    }
}

impl TableRepository for MockTableRepository {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, TableRepositoryError> {
        debug!(table_name, "getting table schema from mock table repository");
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock table repository state lock is poisoned"))?;

        state
            .tables
            .get(table_name)
            .map(|table| table.schema.clone())
            .ok_or_else(|| TableRepositoryError::TableNotFound {
                table_name: table_name.to_string(),
            })
    }

    fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), TableRepositoryError> {
        debug!(table_name, "updating table schema in mock table repository");
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("mock table repository state lock is poisoned"))?;

        let table = state.tables.get_mut(table_name).ok_or_else(|| {
            TableRepositoryError::TableNotFound {
                table_name: table_name.to_string(),
            }
        })?;

        table.schema = schema;
        self.save(&state)?;

        Ok(())
    }
}

impl MockState {
    fn initial() -> Self {
        let table = MockTable {
            name: DUMMY_TABLE.to_string(),
            schema: initial_dummy_table_schema(),
        };
        let mut tables = HashMap::new();
        tables.insert(table.name.clone(), table);

        Self { tables }
    }
}
