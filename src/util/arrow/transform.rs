use arrow::array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use thiserror::Error;

const SECONDS_PER_HOUR: i64 = 60 * 60;
const MILLIS_PER_HOUR: i64 = SECONDS_PER_HOUR * 1_000;
const MICROS_PER_HOUR: i64 = MILLIS_PER_HOUR * 1_000;
const NANOS_PER_HOUR: i64 = MICROS_PER_HOUR * 1_000;

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
                .map(|value| truncate_to_hour(*value, SECONDS_PER_HOUR) * 1_000_000)
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
                .map(|value| truncate_to_hour(*value, MILLIS_PER_HOUR) * 1_000)
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
                .map(|value| truncate_to_hour(*value, MICROS_PER_HOUR))
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
                .map(|value| truncate_to_hour(*value, NANOS_PER_HOUR) / 1_000)
                .collect()
        }
        _ => {
            return Err(TransformError());
        }
    };

    Ok(TimestampMicrosecondArray::from(values))
}

fn truncate_to_hour(value: i64, units_per_hour: i64) -> i64 {
    value.div_euclid(units_per_hour) * units_per_hour
}
