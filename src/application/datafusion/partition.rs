use crate::domain::port::catalog::{
    BoundInclusivity, PartitionTimeBound, PartitionTimeFilter, PartitionTimePredicate,
    PartitionTimeRange,
};
use crate::domain::table::Table;
use crate::domain::time_range::{
    ExpandedPartitionTimes, TimeRange, TimeRangeBound, TimeRangeVec, intersect_optional_ranges,
};
use crate::util::time::truncate_microsecond_to_hour;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Operator;
use datafusion::prelude::Expr;

pub(super) fn extract_partition_times(
    table: &Table,
    filters: &[Expr],
) -> DataFusionResult<Option<PartitionTimeFilter>> {
    let partition_ranges = extract_partition_time_ranges(table, filters)?;
    let Some(partition_ranges) = partition_ranges else {
        return Ok(None);
    };
    if partition_ranges.is_empty() {
        return Ok(None);
    }

    let partition_times = match partition_ranges.to_expanded_partition_times() {
        ExpandedPartitionTimes::Ranges(ranges) => PartitionTimeFilter {
            predicates: ranges.into_iter().map(range_to_predicate).collect(),
        },
        ExpandedPartitionTimes::OpenStart { end } => PartitionTimeFilter {
            predicates: vec![PartitionTimePredicate::Range(PartitionTimeRange {
                lower: None,
                upper: Some(bound_to_partition_bound(end)),
            })],
        },
        ExpandedPartitionTimes::OpenEnd { start } => PartitionTimeFilter {
            predicates: vec![PartitionTimePredicate::Range(PartitionTimeRange {
                lower: Some(bound_to_partition_bound(start)),
                upper: None,
            })],
        },
        ExpandedPartitionTimes::FullyOpen => PartitionTimeFilter::default(),
    };

    Ok(Some(partition_times))
}

fn range_to_predicate(range: TimeRange) -> PartitionTimePredicate {
    if let (Some(start), Some(end)) = (range.start, range.end)
        && start.hour_micros == end.hour_micros
        && start.inclusivity == BoundInclusivity::Inclusive
        && end.inclusivity == BoundInclusivity::Inclusive
    {
        return PartitionTimePredicate::In(vec![start.hour_micros]);
    }

    PartitionTimePredicate::Range(PartitionTimeRange {
        lower: range.start.map(bound_to_partition_bound),
        upper: range.end.map(bound_to_partition_bound),
    })
}

fn bound_to_partition_bound(bound: TimeRangeBound) -> PartitionTimeBound {
    PartitionTimeBound {
        time: bound.hour_micros,
        inclusivity: bound.inclusivity,
    }
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

    Some(TimeRange::new(
        Some(truncate_microsecond_to_hour(low)),
        Some(truncate_microsecond_to_hour(high)),
    ))
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
    let hour = truncate_microsecond_to_hour(value);
    match (side, op) {
        (ComparisonSide::LeftColumn, Operator::Eq)
        | (ComparisonSide::RightColumn, Operator::Eq) => {
            Ok(TimeRange::new(Some(hour), Some(hour)).convert_to_range_vec())
        }
        (ComparisonSide::LeftColumn, Operator::Gt)
        | (ComparisonSide::LeftColumn, Operator::GtEq)
        | (ComparisonSide::RightColumn, Operator::Lt)
        | (ComparisonSide::RightColumn, Operator::LtEq) => {
            Ok(TimeRange::new_lower(hour, BoundInclusivity::Inclusive).convert_to_range_vec())
        }
        (ComparisonSide::LeftColumn, Operator::Lt)
        | (ComparisonSide::LeftColumn, Operator::LtEq)
        | (ComparisonSide::RightColumn, Operator::Gt)
        | (ComparisonSide::RightColumn, Operator::GtEq) => {
            Ok(TimeRange::new_upper(hour, BoundInclusivity::Inclusive).convert_to_range_vec())
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::table::Table;
    use crate::domain::table_schema::TableSchema;
    use datafusion::prelude::{col, lit};
    use rstest::rstest;

    const PARTITION_COLUMN: &str = "posted_at";
    const HOUR_MICROS: i64 = 60 * 60 * 1_000_000;

    fn table() -> Table {
        Table::new(TableSchema::new(
            "hello_table".into(),
            "my_bucket".into(),
            "path/prefix".into(),
            vec![],
        ))
    }

    fn timestamp_micros(value: i64) -> Expr {
        lit(ScalarValue::TimestampMicrosecond(Some(value), None))
    }

    fn bound(time: i64, inclusivity: BoundInclusivity) -> PartitionTimeBound {
        PartitionTimeBound { time, inclusivity }
    }

    fn range(
        lower: Option<(i64, BoundInclusivity)>,
        upper: Option<(i64, BoundInclusivity)>,
    ) -> PartitionTimePredicate {
        PartitionTimePredicate::Range(PartitionTimeRange {
            lower: lower.map(|(time, inclusivity)| bound(time, inclusivity)),
            upper: upper.map(|(time, inclusivity)| bound(time, inclusivity)),
        })
    }

    fn filter(predicates: Vec<PartitionTimePredicate>) -> PartitionTimeFilter {
        PartitionTimeFilter { predicates }
    }

    #[rstest]
    #[case::eq_uses_in(
        col(PARTITION_COLUMN).eq(timestamp_micros(HOUR_MICROS + 1)),
        filter(vec![PartitionTimePredicate::In(vec![HOUR_MICROS])]),
    )]
    #[case::gt_uses_inclusive_lower_bound(
        col(PARTITION_COLUMN).gt(timestamp_micros(HOUR_MICROS + 1)),
        filter(vec![range(Some((HOUR_MICROS, BoundInclusivity::Inclusive)), None)]),
    )]
    #[case::gte_uses_inclusive_lower_bound(
        col(PARTITION_COLUMN).gt_eq(timestamp_micros(HOUR_MICROS + 1)),
        filter(vec![range(Some((HOUR_MICROS, BoundInclusivity::Inclusive)), None)]),
    )]
    #[case::lt_uses_inclusive_upper_bound(
        col(PARTITION_COLUMN).lt(timestamp_micros((2 * HOUR_MICROS) + 1)),
        filter(vec![range(None, Some((2 * HOUR_MICROS, BoundInclusivity::Inclusive)))]),
    )]
    #[case::lte_uses_inclusive_upper_bound(
        col(PARTITION_COLUMN).lt_eq(timestamp_micros((2 * HOUR_MICROS) + 1)),
        filter(vec![range(None, Some((2 * HOUR_MICROS, BoundInclusivity::Inclusive)))]),
    )]
    #[case::between_uses_inclusive_bounds(
        col(PARTITION_COLUMN).between(
            timestamp_micros(HOUR_MICROS + 1),
            timestamp_micros((2 * HOUR_MICROS) + 1),
        ),
        filter(vec![range(
            Some((HOUR_MICROS, BoundInclusivity::Inclusive)),
            Some((2 * HOUR_MICROS, BoundInclusivity::Inclusive)),
        )]),
    )]
    fn extracts_partition_time_filter(#[case] expr: Expr, #[case] expected: PartitionTimeFilter) {
        assert_eq!(
            extract_partition_times(&table(), &[expr]).expect("filter extraction succeeds"),
            Some(expected)
        );
    }
}
