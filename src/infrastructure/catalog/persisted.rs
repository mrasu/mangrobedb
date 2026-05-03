use crate::domain::table_mapping::{MappingStrategy, TableMapping};
use crate::domain::table_schema::{InternalColumnDefinition, PublicColumnDefinition, TableSchema};
use crate::infrastructure::catalog::mock::{MockState, MockTable};
use anyhow::anyhow;
use arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PersistedState {
    tables: Vec<PersistedTable>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedTable {
    name: String,
    public_columns: Vec<PersistedPublicColumn>,
    stream_id_mapping: PersistedTableMapping,
    partition_time_mapping: PersistedTableMapping,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedTableMapping {
    src_column: PersistedPublicColumn,
    dst_column: PersistedInternalColumn,
    strategy: PersistedMappingStrategy,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedPublicColumn {
    name: String,
    data_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedInternalColumn {
    name: String,
    data_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum PersistedMappingStrategy {
    Copy,
    ToHour,
}

impl PersistedState {
    pub(super) fn try_into_state(self) -> anyhow::Result<MockState> {
        let mut tables = HashMap::new();
        for persisted_table in self.tables {
            let table = persisted_table.try_into_table()?;
            tables.insert(table.name.clone(), table);
        }

        Ok(MockState { tables })
    }

    pub(super) fn try_from_state(state: &MockState) -> anyhow::Result<Self> {
        let mut tables = state.tables.values().collect::<Vec<_>>();
        tables.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(Self {
            tables: tables
                .into_iter()
                .map(PersistedTable::try_from_table)
                .collect::<anyhow::Result<Vec<_>>>()?,
        })
    }
}

impl PersistedTable {
    fn try_into_table(self) -> anyhow::Result<MockTable> {
        let columns = self
            .public_columns
            .into_iter()
            .map(PersistedPublicColumn::try_into_public_column)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let stream_id_mapping = self.stream_id_mapping.try_into_table_mapping()?;
        let partition_time_mapping = self.partition_time_mapping.try_into_table_mapping()?;

        Ok(MockTable {
            name: self.name.clone(),
            schema: TableSchema::new(
                self.name,
                columns,
                stream_id_mapping,
                partition_time_mapping,
            ),
        })
    }

    fn try_from_table(table: &MockTable) -> anyhow::Result<Self> {
        let mut public_columns = table
            .schema
            .public_columns()
            .iter()
            .map(PersistedPublicColumn::try_from_public_column)
            .collect::<anyhow::Result<Vec<_>>>()?;
        public_columns.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(Self {
            name: table.name.clone(),
            public_columns,
            stream_id_mapping: PersistedTableMapping::try_from_table_mapping(
                table.schema.stream_id_mapping(),
            )?,
            partition_time_mapping: PersistedTableMapping::try_from_table_mapping(
                table.schema.partition_time_mapping(),
            )?,
        })
    }
}

impl PersistedTableMapping {
    fn try_into_table_mapping(self) -> anyhow::Result<TableMapping> {
        Ok(TableMapping::new(
            self.src_column.try_into_public_column()?,
            self.dst_column.try_into_internal_column()?,
            self.strategy.into_mapping_strategy(),
        ))
    }

    fn try_from_table_mapping(mapping: &TableMapping) -> anyhow::Result<Self> {
        Ok(Self {
            src_column: PersistedPublicColumn::try_from_public_column(mapping.src_column_ref())?,
            dst_column: PersistedInternalColumn::try_from_internal_column(
                mapping.dst_column_ref(),
            )?,
            strategy: PersistedMappingStrategy::from_mapping_strategy(mapping.strategy()),
        })
    }
}

impl PersistedPublicColumn {
    fn try_into_public_column(self) -> anyhow::Result<PublicColumnDefinition> {
        Ok(PublicColumnDefinition::new(
            &self.name,
            parse_data_type(&self.data_type)?,
        ))
    }

    fn try_from_public_column(column: &PublicColumnDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            name: column.name.clone(),
            data_type: format_data_type(column.data_type())?,
        })
    }
}

impl PersistedInternalColumn {
    fn try_into_internal_column(self) -> anyhow::Result<InternalColumnDefinition> {
        Ok(InternalColumnDefinition::new(
            &self.name,
            parse_data_type(&self.data_type)?,
        ))
    }

    fn try_from_internal_column(column: &InternalColumnDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            name: column.name.clone(),
            data_type: format_data_type(column.data_type())?,
        })
    }
}

impl PersistedMappingStrategy {
    fn into_mapping_strategy(self) -> MappingStrategy {
        match self {
            Self::Copy => MappingStrategy::Copy,
            Self::ToHour => MappingStrategy::ToHour,
        }
    }

    fn from_mapping_strategy(strategy: &MappingStrategy) -> Self {
        match strategy {
            MappingStrategy::Copy => Self::Copy,
            MappingStrategy::ToHour => Self::ToHour,
        }
    }
}

fn parse_data_type(value: &str) -> anyhow::Result<DataType> {
    match value {
        "Int32" => Ok(DataType::Int32),
        "Utf8" => Ok(DataType::Utf8),
        "Timestamp(Microsecond,None)" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        other => Err(anyhow!("unsupported persisted Arrow data type: {other}")),
    }
}

fn format_data_type(data_type: &DataType) -> anyhow::Result<String> {
    match data_type {
        DataType::Int32 => Ok("Int32".to_string()),
        DataType::Utf8 => Ok("Utf8".to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok("Timestamp(Microsecond,None)".to_string())
        }
        other => Err(anyhow!(
            "unsupported Arrow data type for mock persistence: {other:?}"
        )),
    }
}
