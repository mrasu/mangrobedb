use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, TimestampMicrosecondArray};
use arrow::compute::{max, min};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, PartialEq)]
pub struct FileStatistics {
    pub row_count: usize,
    pub columns: Vec<ColumnStatistics>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub min: StatisticValue,
    pub max: StatisticValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatisticValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    TimestampMicros(i64),
}

impl FileStatistics {
    pub fn calculate(batch: &RecordBatch) -> Self {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .filter_map(|(field, array)| ColumnStatistics::calculate(field, array))
            .collect();

        Self {
            row_count: batch.num_rows(),
            columns,
        }
    }
}

impl ColumnStatistics {
    pub fn calculate(field: &Field, array: &ArrayRef) -> Option<Self> {
        match field.data_type() {
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>()?;
                Self::calculate_int32(field.name(), array)
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>()?;
                Self::calculate_int64(field.name(), array)
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>()?;
                Self::calculate_float64(field.name(), array)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                Self::calculate_timestamp_microsecond(field.name(), array)
            }
            _ => None,
        }
    }

    fn calculate_int32(column_name: &str, array: &Int32Array) -> Option<Self> {
        Some(Self {
            column_name: column_name.to_string(),
            min: StatisticValue::Int32(min(array)?),
            max: StatisticValue::Int32(max(array)?),
        })
    }

    fn calculate_int64(column_name: &str, array: &Int64Array) -> Option<Self> {
        Some(Self {
            column_name: column_name.to_string(),
            min: StatisticValue::Int64(min(array)?),
            max: StatisticValue::Int64(max(array)?),
        })
    }

    fn calculate_float64(column_name: &str, array: &Float64Array) -> Option<Self> {
        Some(Self {
            column_name: column_name.to_string(),
            min: StatisticValue::Float64(min(array)?),
            max: StatisticValue::Float64(max(array)?),
        })
    }

    fn calculate_timestamp_microsecond(
        column_name: &str,
        array: &TimestampMicrosecondArray,
    ) -> Option<Self> {
        Some(Self {
            column_name: column_name.to_string(),
            min: StatisticValue::TimestampMicros(min(array)?),
            max: StatisticValue::TimestampMicros(max(array)?),
        })
    }
}
