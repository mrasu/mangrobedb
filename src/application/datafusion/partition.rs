use crate::domain::table::Table;
use crate::domain::time_range::{
    ExpandedPartitionTimes, TimeRange, TimeRangeVec, intersect_optional_ranges,
};
use crate::util::time::truncate_microsecond_to_hour;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Operator;
use datafusion::prelude::Expr;

pub(super) fn extract_partition_times(
    table: &Table,
    filters: &[Expr],
) -> DataFusionResult<Option<Vec<i64>>> {
    let partition_ranges = extract_partition_time_ranges(table, filters)?;
    let Some(partition_ranges) = partition_ranges else {
        return Ok(None);
    };
    if partition_ranges.is_empty() {
        return Ok(None);
    }

    let partition_times = match partition_ranges.to_expanded_partition_times() {
        ExpandedPartitionTimes::Bounded(partition_times) => partition_times,
        // `GetCurrentState.partition_times` cannot express open-ended predicates yet,
        // so these cases currently fall back to scanning all visible partitions.
        ExpandedPartitionTimes::OpenStart { .. }
        | ExpandedPartitionTimes::OpenEnd { .. }
        | ExpandedPartitionTimes::FullyOpen => Vec::new(),
    };

    Ok(Some(partition_times))
}

fn extract_partition_time_ranges(
    table: &Table,
    filters: &[Expr],
) -> DataFusionResult<Option<TimeRangeVec>> {
    let partition_source_name = &table.schema.partition_time_mapping().src_column_ref().name;

    let result = filters
        .iter()
        .try_fold(Some(TimeRangeVec::new_full_open()), |acc, expr| {
            let current = extract_partition_time_ranges_from_expr(expr, partition_source_name)?;
            Ok::<_, DataFusionError>(intersect_optional_ranges(acc, current))
        })?;

    Ok(result)
}

fn extract_partition_time_ranges_from_expr(
    expr: &Expr,
    partition_source_name: &str,
) -> DataFusionResult<Option<TimeRangeVec>> {
    match expr {
        Expr::Between(between) => {
            let res = extract_between_partition_time_range(between, partition_source_name)
                .map(|range| range.convert_to_range_vec());
            Ok(res)
        }
        Expr::BinaryExpr(binary) => {
            extract_binary_partition_time_ranges(binary, partition_source_name)
        }
        // TODO: support more complex condition.
        _ => Ok(None),
    }
}

fn extract_between_partition_time_range(
    between: &datafusion::logical_expr::Between,
    partition_source_name: &str,
) -> Option<TimeRange> {
    if !is_expr_partition_time_column(&between.expr, partition_source_name) {
        return None;
    }

    if between.negated {
        // TODO: support negation
        return Some(TimeRange::new_full_open());
    }

    let low = expr_as_timestamp_micros(&between.low)?;
    let high = expr_as_timestamp_micros(&between.high)?;

    Some(TimeRange {
        start_hour_micros: Some(truncate_microsecond_to_hour(low)),
        end_hour_micros: Some(truncate_microsecond_to_hour(high)),
    })
}

fn extract_binary_partition_time_ranges(
    binary: &datafusion::logical_expr::BinaryExpr,
    partition_source_name: &str,
) -> DataFusionResult<Option<TimeRangeVec>> {
    match binary.op {
        Operator::And => {
            let left =
                extract_partition_time_ranges_from_expr(&binary.left, partition_source_name)?;
            let right =
                extract_partition_time_ranges_from_expr(&binary.right, partition_source_name)?;
            return Ok(intersect_optional_ranges(left, right));
        }
        Operator::Or => {
            let left =
                extract_partition_time_ranges_from_expr(&binary.left, partition_source_name)?;
            let right =
                extract_partition_time_ranges_from_expr(&binary.right, partition_source_name)?;
            let Some(left) = left else {
                return Ok(None);
            };
            let Some(right) = right else {
                return Ok(None);
            };

            return Ok(Some(left.union(right)));
        }
        _ => {}
    }

    if is_expr_partition_time_column(&binary.left, partition_source_name) {
        let Some(value) = expr_as_timestamp_micros(&binary.right) else {
            return Err(DataFusionError::Plan(format!(
                "partition_column must compare with timestamp columns. {:?}",
                binary
            )));
        };

        let res = partition_range_from_comparison(&binary.op, value, ComparisonSide::LeftColumn)?;
        return Ok(Some(res));
    }

    if is_expr_partition_time_column(&binary.right, partition_source_name) {
        let Some(value) = expr_as_timestamp_micros(&binary.left) else {
            return Err(DataFusionError::Plan(format!(
                "partition_column must compare with timestamp columns. {:?}",
                binary
            )));
        };

        let res = partition_range_from_comparison(&binary.op, value, ComparisonSide::RightColumn)?;
        return Ok(Some(res));
    }

    Ok(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComparisonSide {
    LeftColumn,
    RightColumn,
}

fn partition_range_from_comparison(
    op: &Operator,
    value: i64,
    side: ComparisonSide,
) -> DataFusionResult<TimeRangeVec> {
    match (side, op) {
        (ComparisonSide::LeftColumn, Operator::Eq)
        | (ComparisonSide::RightColumn, Operator::Eq) => {
            let hour = truncate_microsecond_to_hour(value);
            Ok(TimeRange::new(Some(hour), Some(hour)).convert_to_range_vec())
        }
        (ComparisonSide::LeftColumn, Operator::Gt)
        | (ComparisonSide::LeftColumn, Operator::GtEq)
        | (ComparisonSide::RightColumn, Operator::Lt)
        | (ComparisonSide::RightColumn, Operator::LtEq) => Ok(TimeRange::new(
            Some(truncate_microsecond_to_hour(value)),
            None,
        )
        .convert_to_range_vec()),
        (ComparisonSide::LeftColumn, Operator::Lt)
        | (ComparisonSide::LeftColumn, Operator::LtEq)
        | (ComparisonSide::RightColumn, Operator::Gt)
        | (ComparisonSide::RightColumn, Operator::GtEq) => Ok(TimeRange::new(
            None,
            Some(truncate_microsecond_to_hour(value)),
        )
        .convert_to_range_vec()),
        _ => Err(DataFusionError::Plan(format!(
            "partition_column must use between, <, >, or = operator. {:?}",
            op
        ))),
    }
}

fn is_expr_partition_time_column(expr: &Expr, column_name: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == column_name,
        _ => false,
    }
}

fn expr_as_timestamp_micros(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(value, _) => scalar_value_as_timestamp_micros(value),
        _ => None,
    }
}

fn scalar_value_as_timestamp_micros(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampMicrosecond(Some(value), _) => Some(*value),
        ScalarValue::TimestampMillisecond(Some(value), _) => Some(value * 1_000),
        ScalarValue::TimestampSecond(Some(value), _) => Some(value * 1_000_000),
        ScalarValue::TimestampNanosecond(Some(value), _) => Some(value / 1_000),
        _ => None,
    }
}
