use crate::domain::partition::Partition;
use crate::domain::partition_filter::PartitionFilter;
use crate::domain::partition_range::{
    BoundInclusivity, PartitionRange, PartitionRangeVec, intersect_optional_ranges,
};
use crate::domain::table::Table;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Operator;
use datafusion::prelude::Expr;

pub(super) fn extract_partition_filter(
    table: &Table,
    filters: &[Expr],
) -> DataFusionResult<Option<PartitionFilter>> {
    let partition_ranges = extract_partition_ranges(table, filters)?;
    let Some(partition_ranges) = partition_ranges else {
        return Ok(None);
    };
    if partition_ranges.is_empty() {
        return Ok(None);
    }

    let filter =
        PartitionFilter::new_from_expanded_partition(partition_ranges.to_expanded_partitions());
    Ok(Some(filter))
}

fn extract_partition_ranges(
    table: &Table,
    filters: &[Expr],
) -> DataFusionResult<Option<PartitionRangeVec>> {
    let partition_column = &table.schema.partition_column();

    let result =
        filters
            .iter()
            .try_fold(Some(PartitionRangeVec::new_full_open()), |acc, expr| {
                let current = extract_partition_ranges_from_expr(expr, partition_column)?;
                Ok::<_, DataFusionError>(intersect_optional_ranges(acc, current))
            })?;

    Ok(result)
}

fn extract_partition_ranges_from_expr(
    expr: &Expr,
    partition_source_name: &str,
) -> DataFusionResult<Option<PartitionRangeVec>> {
    match expr {
        Expr::Between(between) => {
            let res = extract_between_partition_range(between, partition_source_name)
                .map(|range| range.convert_to_range_vec());
            Ok(res)
        }
        Expr::BinaryExpr(binary) => extract_binary_partition_ranges(binary, partition_source_name),
        // TODO: support more complex condition.
        _ => Ok(None),
    }
}

fn extract_between_partition_range(
    between: &datafusion::logical_expr::Between,
    partition_source_name: &str,
) -> Option<PartitionRange> {
    if !is_expr_partition_column(&between.expr, partition_source_name) {
        return None;
    }

    if between.negated {
        // TODO: support negation
        return Some(PartitionRange::new_full_open());
    }

    let low = expr_as_partition(&between.low)?;
    let high = expr_as_partition(&between.high)?;

    Some(PartitionRange::new(Some(low), Some(high)))
}

fn extract_binary_partition_ranges(
    binary: &datafusion::logical_expr::BinaryExpr,
    partition_source_name: &str,
) -> DataFusionResult<Option<PartitionRangeVec>> {
    match binary.op {
        Operator::And => {
            let left = extract_partition_ranges_from_expr(&binary.left, partition_source_name)?;
            let right = extract_partition_ranges_from_expr(&binary.right, partition_source_name)?;
            return Ok(intersect_optional_ranges(left, right));
        }
        Operator::Or => {
            let left = extract_partition_ranges_from_expr(&binary.left, partition_source_name)?;
            let right = extract_partition_ranges_from_expr(&binary.right, partition_source_name)?;
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

    if is_expr_partition_column(&binary.left, partition_source_name) {
        let Some(value) = expr_as_partition(&binary.right) else {
            return Err(DataFusionError::Plan(format!(
                "partition_column must compare with timestamp columns. {:?}",
                binary
            )));
        };

        let res = partition_range_from_comparison(&binary.op, value, ComparisonSide::LeftColumn)?;
        return Ok(Some(res));
    }

    if is_expr_partition_column(&binary.right, partition_source_name) {
        let Some(value) = expr_as_partition(&binary.left) else {
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
    partition: Partition,
    side: ComparisonSide,
) -> DataFusionResult<PartitionRangeVec> {
    match (side, op) {
        (ComparisonSide::LeftColumn, Operator::Eq)
        | (ComparisonSide::RightColumn, Operator::Eq) => {
            Ok(PartitionRange::new(Some(partition), Some(partition)).convert_to_range_vec())
        }
        (ComparisonSide::LeftColumn, Operator::Gt)
        | (ComparisonSide::RightColumn, Operator::Lt) => Ok(PartitionRange::new_lower(
            partition,
            BoundInclusivity::Exclusive,
        )
        .convert_to_range_vec()),
        (ComparisonSide::LeftColumn, Operator::GtEq)
        | (ComparisonSide::RightColumn, Operator::LtEq) => Ok(PartitionRange::new_lower(
            partition,
            BoundInclusivity::Inclusive,
        )
        .convert_to_range_vec()),
        (ComparisonSide::LeftColumn, Operator::Lt)
        | (ComparisonSide::RightColumn, Operator::Gt) => Ok(PartitionRange::new_upper(
            partition,
            BoundInclusivity::Exclusive,
        )
        .convert_to_range_vec()),
        (ComparisonSide::LeftColumn, Operator::LtEq)
        | (ComparisonSide::RightColumn, Operator::GtEq) => Ok(PartitionRange::new_upper(
            partition,
            BoundInclusivity::Inclusive,
        )
        .convert_to_range_vec()),
        _ => Err(DataFusionError::Plan(format!(
            "partition_column must use between, <, >, or = operator. {:?}",
            op
        ))),
    }
}

fn is_expr_partition_column(expr: &Expr, column_name: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == column_name,
        _ => false,
    }
}

fn expr_as_partition(expr: &Expr) -> Option<Partition> {
    match expr {
        Expr::Literal(value, _) => scalar_value_as_partition(value),
        _ => None,
    }
}

fn scalar_value_as_partition(value: &ScalarValue) -> Option<Partition> {
    match value {
        ScalarValue::Int64(Some(value)) => Some(Partition::Int64(*value)),
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            Some(Partition::TimeMicrosecond(*value))
        }
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(Partition::TimeMicrosecond(value * 1_000))
        }
        ScalarValue::TimestampSecond(Some(value), _) => {
            Some(Partition::TimeMicrosecond(value * 1_000_000))
        }
        ScalarValue::TimestampNanosecond(Some(value), _) => {
            Some(Partition::TimeMicrosecond(value / 1_000))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::column_data_type::ColumnDataType;
    use crate::domain::column_data_type::TimeUnit::Microsecond;
    use crate::domain::file::FileFormat;
    use crate::domain::partition_filter::PartitionPredicate;
    use crate::domain::partition_range::PartitionRangeBound;
    use crate::domain::table::Table;
    use crate::domain::table_schema::{
        ExternalLocation, ExternalLocationScheme, PublicColumnDefinition, TableSchema,
    };
    use datafusion::prelude::{col, lit};
    use rstest::rstest;

    const PARTITION_COLUMN: &str = "posted_at";
    const HOUR_MICROS: i64 = 60 * 60 * 1_000_000;

    fn table() -> Table {
        Table::new(
            TableSchema::try_new(
                "hello_table".into(),
                ExternalLocation::new(
                    ExternalLocationScheme::S3,
                    "my_bucket".into(),
                    "path/prefix".into(),
                    None,
                    None,
                ),
                FileFormat::Vortex,
                vec![
                    PublicColumnDefinition::new("stream_column", ColumnDataType::Int64, true, None),
                    PublicColumnDefinition::new(
                        PARTITION_COLUMN,
                        ColumnDataType::Timestamp(Microsecond),
                        true,
                        None,
                    ),
                ],
                "stream_column".into(),
                "posted_at".into(),
                None,
            )
            .unwrap(),
        )
    }

    fn timestamp_micros(value: i64) -> Expr {
        lit(ScalarValue::TimestampMicrosecond(Some(value), None))
    }

    fn bound(time: i64, inclusivity: BoundInclusivity) -> PartitionRangeBound {
        PartitionRangeBound {
            partition: Partition::TimeMicrosecond(time),
            inclusivity,
        }
    }

    fn range(
        lower: Option<(i64, BoundInclusivity)>,
        upper: Option<(i64, BoundInclusivity)>,
    ) -> PartitionPredicate {
        PartitionPredicate::Range(PartitionRange {
            lower: lower.map(|(time, inclusivity)| bound(time, inclusivity)),
            upper: upper.map(|(time, inclusivity)| bound(time, inclusivity)),
        })
    }

    fn filter(predicates: Vec<PartitionPredicate>) -> PartitionFilter {
        PartitionFilter { predicates }
    }

    #[rstest]
    #[case::eq_uses_in(
        col(PARTITION_COLUMN).eq(timestamp_micros(HOUR_MICROS)),
        filter(vec![PartitionPredicate::In(vec![Partition::TimeMicrosecond(HOUR_MICROS)])]),
    )]
    #[case::gt_uses_inclusive_lower_bound(
        col(PARTITION_COLUMN).gt(timestamp_micros(HOUR_MICROS)),
        filter(vec![range(Some((HOUR_MICROS, BoundInclusivity::Exclusive)), None)]),
    )]
    #[case::gte_uses_inclusive_lower_bound(
        col(PARTITION_COLUMN).gt_eq(timestamp_micros(HOUR_MICROS)),
        filter(vec![range(Some((HOUR_MICROS, BoundInclusivity::Inclusive)), None)]),
    )]
    #[case::lt_uses_inclusive_upper_bound(
        col(PARTITION_COLUMN).lt(timestamp_micros(2 * HOUR_MICROS)),
        filter(vec![range(None, Some((2 * HOUR_MICROS, BoundInclusivity::Exclusive)))]),
    )]
    #[case::lte_uses_inclusive_upper_bound(
        col(PARTITION_COLUMN).lt_eq(timestamp_micros(2 * HOUR_MICROS)),
        filter(vec![range(None, Some((2 * HOUR_MICROS, BoundInclusivity::Inclusive)))]),
    )]
    #[case::between_uses_inclusive_bounds(
        col(PARTITION_COLUMN).between(
            timestamp_micros(HOUR_MICROS),
            timestamp_micros(2 * HOUR_MICROS),
        ),
        filter(vec![range(
            Some((HOUR_MICROS, BoundInclusivity::Inclusive)),
            Some((2 * HOUR_MICROS, BoundInclusivity::Inclusive)),
        )]),
    )]
    fn extracts_partition_filter(#[case] expr: Expr, #[case] expected: PartitionFilter) {
        assert_eq!(
            extract_partition_filter(&table(), &[expr]).expect("filter extraction succeeds"),
            Some(expected)
        );
    }
}
