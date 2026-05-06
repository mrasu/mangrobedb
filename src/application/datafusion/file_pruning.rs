use std::collections::HashMap;

use crate::domain::port::catalog::CatalogError;
use crate::domain::port::catalog::{CatalogFile, CatalogFileInfo, CatalogPort};
use crate::domain::statistics::{ColumnStatistics, StatisticValue};
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

pub(crate) fn prune_files_by_statistics<C: CatalogPort>(
    catalog_port: &C,
    table_name: &str,
    files: &[CatalogFile],
    filters: &[Expr],
) -> Result<Vec<CatalogFile>, CatalogError> {
    let file_ids = files
        .iter()
        .map(|file| file.file_id.clone())
        .collect::<Vec<_>>();
    let file_info_by_id = catalog_port.get_file_info(table_name, &file_ids)?;

    Ok(files
        .iter()
        .filter(|file| {
            let Some(file_info) = file_info_by_id.get(&file.file_id) else {
                return false;
            };
            file_matches_all_filters(file_info, filters)
        })
        .cloned()
        .collect())
}

fn file_matches_all_filters(file_info: &CatalogFileInfo, filters: &[Expr]) -> bool {
    let column_statistics_by_name = file_info
        .column_statistics
        .iter()
        .map(|column| (column.column_name.as_str(), column))
        .collect::<HashMap<_, _>>();

    filters
        .iter()
        .all(|filter| file_may_match_expr(&column_statistics_by_name, filter))
}

fn file_may_match_expr(
    column_statistics_by_name: &HashMap<&str, &ColumnStatistics>,
    expr: &Expr,
) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => file_may_match_binary_expr(column_statistics_by_name, binary),
        Expr::Between(between) => file_may_match_between_expr(column_statistics_by_name, between),
        // TODO: support more expression kinds for statistics pruning.
        _ => true,
    }
}

fn file_may_match_binary_expr(
    column_statistics_by_name: &HashMap<&str, &ColumnStatistics>,
    binary: &BinaryExpr,
) -> bool {
    match binary.op {
        Operator::And => {
            file_may_match_expr(column_statistics_by_name, &binary.left)
                && file_may_match_expr(column_statistics_by_name, &binary.right)
        }
        Operator::Or => {
            file_may_match_expr(column_statistics_by_name, &binary.left)
                || file_may_match_expr(column_statistics_by_name, &binary.right)
        }
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            if let Some(result) = evaluate_binary_comparison(
                column_statistics_by_name,
                &binary.left,
                &binary.op,
                &binary.right,
            ) {
                return result;
            }

            // Re-check the predicate in the opposite direction so `literal < column`
            // can be evaluated as `column > literal`.
            if let Some(result) = evaluate_binary_comparison(
                column_statistics_by_name,
                &binary.right,
                &swap_comparison_operator(binary.op),
                &binary.left,
            ) {
                return result;
            }

            true
        }
        // TODO: support more comparison operators for statistics pruning.
        _ => true,
    }
}

fn file_may_match_between_expr(
    column_statistics_by_name: &HashMap<&str, &ColumnStatistics>,
    between: &Between,
) -> bool {
    if between.negated {
        // TODO: support NOT BETWEEN statistics pruning.
        return true;
    }

    let Expr::Column(column) = between.expr.as_ref() else {
        // `between.expr` is not optional, but it may be any expression node.
        // For example, it can be a function call or another computed expression
        // instead of a direct column reference.
        return true;
    };
    let Some(column_statistics) = column_statistics_by_name.get(column.name.as_str()).copied()
    else {
        return true;
    };
    let Some(between_low) = statistic_value_from_scalar(&between.low) else {
        return true;
    };
    let Some(between_high) = statistic_value_from_scalar(&between.high) else {
        return true;
    };

    if let Some(column_max) = column_statistics.max.as_ref()
        && column_max < &between_low
    {
        return false;
    }

    if let Some(column_min) = column_statistics.min.as_ref()
        && column_min > &between_high
    {
        return false;
    }

    true
}

fn evaluate_binary_comparison(
    column_statistics_by_name: &HashMap<&str, &ColumnStatistics>,
    maybe_column_expr: &Expr,
    op: &Operator,
    maybe_literal_expr: &Expr,
) -> Option<bool> {
    let Expr::Column(column) = maybe_column_expr else {
        return None;
    };
    let column_statistics = column_statistics_by_name
        .get(column.name.as_str())
        .copied()?;
    let literal_value = statistic_value_from_scalar(maybe_literal_expr)?;

    match op {
        Operator::Eq => {
            if let Some(column_min) = column_statistics.min.as_ref()
                && column_min > &literal_value
            {
                return Some(false);
            }

            if let Some(column_max) = column_statistics.max.as_ref()
                && column_max < &literal_value
            {
                return Some(false);
            }

            Some(true)
        }
        Operator::Lt => {
            let column_min = column_statistics.min.as_ref()?;
            Some(column_min < &literal_value)
        }
        Operator::LtEq => {
            let column_min = column_statistics.min.as_ref()?;
            Some(column_min <= &literal_value)
        }
        Operator::Gt => {
            let column_max = column_statistics.max.as_ref()?;
            Some(column_max > &literal_value)
        }
        Operator::GtEq => {
            let column_max = column_statistics.max.as_ref()?;
            Some(column_max >= &literal_value)
        }
        _ => None,
    }
}

fn statistic_value_from_scalar(expr: &Expr) -> Option<StatisticValue> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };

    match value {
        ScalarValue::Int32(Some(value)) => Some(StatisticValue::Int32(*value)),
        ScalarValue::Int64(Some(value)) => Some(StatisticValue::Int64(*value)),
        ScalarValue::Float64(Some(value)) => Some(StatisticValue::Float64(*value)),
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            Some(StatisticValue::TimestampMicros(*value))
        }
        // Catalog statistics currently store timestamps only as microseconds,
        // so timestamp literals are normalized to microseconds before comparison.
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(StatisticValue::TimestampMicros(*value * 1_000))
        }
        ScalarValue::TimestampSecond(Some(value), _) => {
            Some(StatisticValue::TimestampMicros(*value * 1_000_000))
        }
        ScalarValue::TimestampNanosecond(Some(value), _) => {
            Some(StatisticValue::TimestampMicros(*value / 1_000))
        }
        // TODO: support more literal types for statistics pruning.
        _ => None,
    }
}

fn swap_comparison_operator(op: Operator) -> Operator {
    match op {
        Operator::Eq => Operator::Eq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::file_matches_all_filters;
    use crate::domain::port::catalog::{CatalogFileInfo, FileMetadata};
    use crate::domain::statistics::{ColumnStatistics, StatisticValue};
    use datafusion::prelude::{col, lit};

    #[test]
    fn between_matches_at_both_boundaries() {
        let file_info = build_file_info(Some(10), Some(20));
        let cond =
            file_matches_all_filters(&file_info, &[col("value").between(lit(10i32), lit(20i32))]);

        assert_eq!(true, cond);
    }

    #[test]
    fn between_prunes_when_range_is_below_file_min() {
        let file_info = build_file_info(Some(10), Some(20));
        let cond =
            file_matches_all_filters(&file_info, &[col("value").between(lit(1i32), lit(9i32))]);

        assert_eq!(false, cond);
    }

    #[test]
    fn between_prunes_when_range_is_above_file_max() {
        let file_info = build_file_info(Some(10), Some(20));
        let cond =
            file_matches_all_filters(&file_info, &[col("value").between(lit(21i32), lit(30i32))]);

        assert_eq!(false, cond);
    }

    #[test]
    fn between_uses_min_only_statistics() {
        let file_info = build_file_info(Some(10), None);

        let below_min =
            file_matches_all_filters(&file_info, &[col("value").between(lit(1i32), lit(9i32))]);
        let at_min =
            file_matches_all_filters(&file_info, &[col("value").between(lit(1i32), lit(10i32))]);

        assert_eq!(false, below_min);
        assert_eq!(true, at_min);
    }

    #[test]
    fn between_uses_max_only_statistics() {
        let file_info = build_file_info(None, Some(20));

        let above_max =
            file_matches_all_filters(&file_info, &[col("value").between(lit(21i32), lit(30i32))]);
        let at_max =
            file_matches_all_filters(&file_info, &[col("value").between(lit(20i32), lit(30i32))]);

        assert_eq!(false, above_max);
        assert_eq!(true, at_max);
    }

    #[test]
    fn binary_eq_respects_boundaries() {
        let file_info = build_file_info(Some(10), Some(20));

        let min_boundary = file_matches_all_filters(&file_info, &[col("value").eq(lit(10i32))]);
        let max_boundary = file_matches_all_filters(&file_info, &[col("value").eq(lit(20i32))]);
        let below_min = file_matches_all_filters(&file_info, &[col("value").eq(lit(9i32))]);
        let above_max = file_matches_all_filters(&file_info, &[col("value").eq(lit(21i32))]);

        assert_eq!(true, min_boundary);
        assert_eq!(true, max_boundary);
        assert_eq!(false, below_min);
        assert_eq!(false, above_max);
    }

    #[test]
    fn binary_eq_uses_single_sided_statistics() {
        let min_only_file_info = build_file_info(Some(10), None);
        let max_only_file_info = build_file_info(None, Some(20));

        let below_min =
            file_matches_all_filters(&min_only_file_info, &[col("value").eq(lit(9i32))]);
        let at_min = file_matches_all_filters(&min_only_file_info, &[col("value").eq(lit(10i32))]);
        let above_max =
            file_matches_all_filters(&max_only_file_info, &[col("value").eq(lit(21i32))]);
        let at_max = file_matches_all_filters(&max_only_file_info, &[col("value").eq(lit(20i32))]);

        assert_eq!(false, below_min);
        assert_eq!(true, at_min);
        assert_eq!(false, above_max);
        assert_eq!(true, at_max);
    }

    #[test]
    fn binary_lt_and_lte_respect_boundaries() {
        let file_info = build_file_info(Some(10), Some(20));

        let lt_min = file_matches_all_filters(&file_info, &[col("value").lt(lit(10i32))]);
        let lt_above_min = file_matches_all_filters(&file_info, &[col("value").lt(lit(11i32))]);
        let lte_min = file_matches_all_filters(&file_info, &[col("value").lt_eq(lit(10i32))]);
        let lte_below_min = file_matches_all_filters(&file_info, &[col("value").lt_eq(lit(9i32))]);

        assert_eq!(false, lt_min);
        assert_eq!(true, lt_above_min);
        assert_eq!(true, lte_min);
        assert_eq!(false, lte_below_min);
    }

    #[test]
    fn binary_lt_and_lte_use_min_only_statistics() {
        let file_info = build_file_info(Some(10), None);

        let lt_min = file_matches_all_filters(&file_info, &[col("value").lt(lit(10i32))]);
        let lt_above_min = file_matches_all_filters(&file_info, &[col("value").lt(lit(11i32))]);
        let lte_min = file_matches_all_filters(&file_info, &[col("value").lt_eq(lit(10i32))]);
        let lte_below_min = file_matches_all_filters(&file_info, &[col("value").lt_eq(lit(9i32))]);

        assert_eq!(false, lt_min);
        assert_eq!(true, lt_above_min);
        assert_eq!(true, lte_min);
        assert_eq!(false, lte_below_min);
    }

    #[test]
    fn binary_lt_and_lte_use_max_only_statistics() {
        let file_info = build_file_info(None, Some(20));

        let lt_below_max = file_matches_all_filters(&file_info, &[col("value").lt(lit(19i32))]);
        let lt_max = file_matches_all_filters(&file_info, &[col("value").lt(lit(20i32))]);
        let lte_max = file_matches_all_filters(&file_info, &[col("value").lt_eq(lit(20i32))]);
        let lte_above_max = file_matches_all_filters(&file_info, &[col("value").lt_eq(lit(21i32))]);

        assert_eq!(true, lt_below_max);
        assert_eq!(true, lt_max);
        assert_eq!(true, lte_max);
        assert_eq!(true, lte_above_max);
    }

    #[test]
    fn binary_gt_and_gte_respect_boundaries() {
        let file_info = build_file_info(Some(10), Some(20));

        let gt_max = file_matches_all_filters(&file_info, &[col("value").gt(lit(20i32))]);
        let gt_below_max = file_matches_all_filters(&file_info, &[col("value").gt(lit(19i32))]);
        let gte_max = file_matches_all_filters(&file_info, &[col("value").gt_eq(lit(20i32))]);
        let gte_above_max = file_matches_all_filters(&file_info, &[col("value").gt_eq(lit(21i32))]);

        assert_eq!(false, gt_max);
        assert_eq!(true, gt_below_max);
        assert_eq!(true, gte_max);
        assert_eq!(false, gte_above_max);
    }

    #[test]
    fn binary_gt_and_gte_use_min_only_statistics() {
        let file_info = build_file_info(Some(10), None);

        let gt_below_min = file_matches_all_filters(&file_info, &[col("value").gt(lit(9i32))]);
        let gt_min = file_matches_all_filters(&file_info, &[col("value").gt(lit(10i32))]);
        let gte_below_min = file_matches_all_filters(&file_info, &[col("value").gt_eq(lit(9i32))]);
        let gte_min = file_matches_all_filters(&file_info, &[col("value").gt_eq(lit(10i32))]);

        assert_eq!(true, gt_below_min);
        assert_eq!(true, gt_min);
        assert_eq!(true, gte_below_min);
        assert_eq!(true, gte_min);
    }

    #[test]
    fn binary_gt_and_gte_use_max_only_statistics() {
        let file_info = build_file_info(None, Some(20));

        let gt_max = file_matches_all_filters(&file_info, &[col("value").gt(lit(20i32))]);
        let gt_below_max = file_matches_all_filters(&file_info, &[col("value").gt(lit(19i32))]);
        let gte_max = file_matches_all_filters(&file_info, &[col("value").gt_eq(lit(20i32))]);
        let gte_above_max = file_matches_all_filters(&file_info, &[col("value").gt_eq(lit(21i32))]);

        assert_eq!(false, gt_max);
        assert_eq!(true, gt_below_max);
        assert_eq!(true, gte_max);
        assert_eq!(false, gte_above_max);
    }

    fn build_file_info(min: Option<i32>, max: Option<i32>) -> CatalogFileInfo {
        CatalogFileInfo {
            file_id: "id:test".to_string(),
            path: "test".to_string(),
            size: 1,
            column_statistics: vec![ColumnStatistics {
                column_name: "value".to_string(),
                min: min.map(StatisticValue::Int32),
                max: max.map(StatisticValue::Int32),
            }],
            file_metadata: FileMetadata::default(),
            row_count: 1,
        }
    }
}
