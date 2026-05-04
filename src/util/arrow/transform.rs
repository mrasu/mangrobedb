use arrow::array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use thiserror::Error;

use crate::util::time::{
    truncate_microsecond_to_hour, truncate_millisecond_to_hour, truncate_nanosecond_to_hour,
    truncate_second_to_hour,
};

#[derive(Debug, Error)]
#[error("failed to transform")]
pub struct TransformError();

pub fn create_hour_array<T: Array + ?Sized>(
    array: &T,
) -> Result<TimestampMicrosecondArray, TransformError> {
    let values: Vec<i64> = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("timestamp second datatype must match array type");
            array
                .values()
                .iter()
                .map(|value| truncate_second_to_hour(*value) * 1_000_000)
                .collect()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("timestamp millisecond datatype must match array type");
            array
                .values()
                .iter()
                .map(|value| truncate_millisecond_to_hour(*value) * 1_000)
                .collect()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp microsecond datatype must match array type");
            array
                .values()
                .iter()
                .map(|value| truncate_microsecond_to_hour(*value))
                .collect()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp nanosecond datatype must match array type");
            array
                .values()
                .iter()
                .map(|value| truncate_nanosecond_to_hour(*value) / 1_000)
                .collect()
        }
        _ => {
            return Err(TransformError());
        }
    };

    Ok(TimestampMicrosecondArray::from(values))
}
