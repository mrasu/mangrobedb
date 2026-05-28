use crate::application::datafusion::query::codec::error::{CodecError, validation_error};
use crate::domain::column_data_type::TimeUnit;

pub(super) fn to_domain_time_unit(precision: Option<u64>) -> Result<TimeUnit, CodecError> {
    match precision.unwrap_or(6) {
        0 => Ok(TimeUnit::Second),
        3 => Ok(TimeUnit::Millisecond),
        6 => Ok(TimeUnit::Microsecond),
        9 => Ok(TimeUnit::Nanosecond),
        other => Err(validation_error(format!(
            "unsupported time precision: {other}"
        ))),
    }
}

pub(super) fn from_domain_time_unit(unit: &TimeUnit) -> u64 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}
