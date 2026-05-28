use crate::application::datafusion::query::codec::error::{CodecError, validation_error};
use crate::application::datafusion::query::codec::time_unit::{
    from_domain_time_unit, to_domain_time_unit,
};
use crate::domain::column_data_type::ColumnDataType;
use datafusion::logical_expr::sqlparser::ast::{DataType as SqlDataType, TimezoneInfo};

pub(super) fn to_column_data_type(data_type: &SqlDataType) -> Result<ColumnDataType, CodecError> {
    Ok(match data_type {
        SqlDataType::Bool | SqlDataType::Boolean => ColumnDataType::Bool,
        SqlDataType::Int64 => ColumnDataType::Int64,
        SqlDataType::Float64 => ColumnDataType::Float64,
        SqlDataType::Text => ColumnDataType::String,
        SqlDataType::Date => ColumnDataType::Date,
        SqlDataType::Timestamp(precision, _) => {
            ColumnDataType::Timestamp(to_domain_time_unit(*precision)?)
        }
        _ => {
            return Err(validation_error(format!(
                "unsupported column type: {data_type}"
            )));
        }
    })
}

pub(super) fn from_column_data_type(data_type: &ColumnDataType) -> SqlDataType {
    match data_type {
        ColumnDataType::Bool => SqlDataType::Bool,
        ColumnDataType::Int32 => SqlDataType::Int32,
        ColumnDataType::Int64 => SqlDataType::Int64,
        ColumnDataType::Float64 => SqlDataType::Float64,
        ColumnDataType::String => SqlDataType::Text,
        ColumnDataType::Date => SqlDataType::Date,
        ColumnDataType::Timestamp(unit) => {
            SqlDataType::Timestamp(Some(from_domain_time_unit(unit)), TimezoneInfo::None)
        }
    }
}
