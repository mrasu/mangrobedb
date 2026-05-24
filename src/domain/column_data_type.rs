use crate::domain::table_schema::TableSchemaError;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use datafusion::sql::sqlparser::ast::{DataType as SqlDataType, TimezoneInfo};
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDataType {
    Bool,
    Int32,
    Int64,
    Float64,
    String,
    Date,
    Timestamp(TimeUnit),
}

impl ColumnDataType {
    pub fn display_label(&self) -> String {
        match self {
            ColumnDataType::Bool => SqlDataType::Bool.to_string(),
            ColumnDataType::Int32 => SqlDataType::Int32.to_string(),
            ColumnDataType::Int64 => SqlDataType::Int64.to_string(),
            ColumnDataType::Float64 => SqlDataType::Float64.to_string(),
            ColumnDataType::String => SqlDataType::Text.to_string(),
            ColumnDataType::Date => SqlDataType::Date.to_string(),
            ColumnDataType::Timestamp(TimeUnit::Second) => {
                SqlDataType::Timestamp(Some(0), TimezoneInfo::None).to_string()
            }
            ColumnDataType::Timestamp(TimeUnit::Millisecond) => {
                SqlDataType::Timestamp(Some(3), TimezoneInfo::None).to_string()
            }
            ColumnDataType::Timestamp(TimeUnit::Microsecond) => {
                SqlDataType::Timestamp(Some(6), TimezoneInfo::None).to_string()
            }
            ColumnDataType::Timestamp(TimeUnit::Nanosecond) => {
                SqlDataType::Timestamp(Some(9), TimezoneInfo::None).to_string()
            }
        }
    }
}

impl Display for ColumnDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_label())
    }
}

impl TryFrom<ArrowDataType> for ColumnDataType {
    type Error = TableSchemaError;

    fn try_from(value: ArrowDataType) -> Result<Self, Self::Error> {
        let res = match value {
            ArrowDataType::Boolean => ColumnDataType::Bool,
            ArrowDataType::Int32 => ColumnDataType::Int32,
            ArrowDataType::Int64 => ColumnDataType::Int64,
            ArrowDataType::Float64 => ColumnDataType::Float64,
            ArrowDataType::Utf8 => ColumnDataType::String,
            ArrowDataType::Date64 => ColumnDataType::Date,
            ArrowDataType::Timestamp(unit, _) => match unit {
                Second => ColumnDataType::Timestamp(TimeUnit::Second),
                Millisecond => ColumnDataType::Timestamp(TimeUnit::Millisecond),
                Microsecond => ColumnDataType::Timestamp(TimeUnit::Microsecond),
                Nanosecond => ColumnDataType::Timestamp(TimeUnit::Nanosecond),
            },
            _ => {
                return Err(TableSchemaError::UnsupportedArrowDataType { data_type: value });
            }
        };

        Ok(res)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}
