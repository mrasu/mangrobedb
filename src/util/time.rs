const SECONDS_PER_HOUR: i64 = 60 * 60;
const MILLIS_PER_HOUR: i64 = SECONDS_PER_HOUR * 1_000;
const MICROS_PER_HOUR: i64 = MILLIS_PER_HOUR * 1_000;
const NANOS_PER_HOUR: i64 = MICROS_PER_HOUR * 1_000;

pub fn truncate_second_to_hour(value: i64) -> i64 {
    truncate_to_hour(value, SECONDS_PER_HOUR)
}

pub fn truncate_millisecond_to_hour(value: i64) -> i64 {
    truncate_to_hour(value, MILLIS_PER_HOUR)
}

pub fn truncate_microsecond_to_hour(value: i64) -> i64 {
    truncate_to_hour(value, MICROS_PER_HOUR)
}

pub fn truncate_nanosecond_to_hour(value: i64) -> i64 {
    truncate_to_hour(value, NANOS_PER_HOUR)
}

fn truncate_to_hour(value: i64, units_per_hour: i64) -> i64 {
    value.div_euclid(units_per_hour) * units_per_hour
}
