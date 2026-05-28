use crate::domain::table::Table;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::Expr;

pub(super) fn extract_stream(table: &Table, filters: &[Expr]) -> DataFusionResult<i64> {
    let stream_column = table.schema.stream_column();
    let mut stream = None;

    for filter in filters {
        stream = merge_streams(
            &stream_column,
            stream,
            extract_stream_from_root_expr(filter, &stream_column)?,
        )?;
    }

    stream.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "stream filter is required. {stream_column} must be compared with = and an int64 value"
        ))
    })
}

fn extract_stream_from_root_expr(
    expr: &Expr,
    stream_column: &str,
) -> DataFusionResult<Option<i64>> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => merge_streams(
            stream_column,
            extract_stream_from_root_expr(&binary.left, stream_column)?,
            extract_stream_from_root_expr(&binary.right, stream_column)?,
        ),
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            extract_stream_from_eq(binary, stream_column)
        }
        _ if contains_stream_column(expr, stream_column) => Err(DataFusionError::Plan(format!(
            "stream column must be a root = condition. {expr:?}"
        ))),
        _ => Ok(None),
    }
}

fn extract_stream_from_eq(
    binary: &BinaryExpr,
    stream_column: &str,
) -> DataFusionResult<Option<i64>> {
    if is_stream_column(&binary.left, stream_column) {
        return literal_as_stream(&binary.right, binary);
    }

    if is_stream_column(&binary.right, stream_column) {
        return literal_as_stream(&binary.left, binary);
    }

    if contains_stream_column(&binary.left, stream_column)
        || contains_stream_column(&binary.right, stream_column)
    {
        return Err(DataFusionError::Plan(format!(
            "stream column must be compared directly with an int64 value. {binary:?}"
        )));
    }

    Ok(None)
}

fn merge_streams(
    stream_column: &str,
    left: Option<i64>,
    right: Option<i64>,
) -> DataFusionResult<Option<i64>> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => Err(DataFusionError::Plan(format!(
            "stream column must compare with a single value. {stream_column} = {left} and {stream_column} = {right}"
        ))),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn literal_as_stream(expr: &Expr, binary: &BinaryExpr) -> DataFusionResult<Option<i64>> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(Some(*value)),
        _ => Err(DataFusionError::Plan(format!(
            "stream column must compare with an int64 value. {binary:?}"
        ))),
    }
}

fn contains_stream_column(expr: &Expr, stream_column: &str) -> bool {
    expr.column_refs()
        .iter()
        .any(|column| column.name == stream_column)
}

fn is_stream_column(expr: &Expr, stream_column: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == stream_column,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::column_data_type::ColumnDataType;
    use crate::domain::column_data_type::TimeUnit::Microsecond;
    use crate::domain::file::FileFormat;
    use crate::domain::table::Table;
    use crate::domain::table_schema::{
        ExternalLocation, ExternalLocationScheme, PublicColumnDefinition, TableSchema,
    };
    use datafusion::prelude::{col, lit};

    const STREAM_COLUMN: &str = "stream_column";
    const PARTITION_COLUMN: &str = "posted_at";

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
                    PublicColumnDefinition::new(STREAM_COLUMN, ColumnDataType::Int64, true, None),
                    PublicColumnDefinition::new(
                        PARTITION_COLUMN,
                        ColumnDataType::Timestamp(Microsecond),
                        true,
                        None,
                    ),
                ],
                STREAM_COLUMN.into(),
                PARTITION_COLUMN.into(),
                None,
            )
            .unwrap(),
        )
    }

    #[test]
    fn extracts_stream_from_left_column_equality() {
        assert_eq!(
            extract_stream(&table(), &[col(STREAM_COLUMN).eq(lit(42_i64))])
                .expect("stream extraction succeeds"),
            42
        );
    }

    #[test]
    fn extracts_stream_from_right_column_equality() {
        assert_eq!(
            extract_stream(&table(), &[lit(42_i64).eq(col(STREAM_COLUMN))])
                .expect("stream extraction succeeds"),
            42
        );
    }

    #[test]
    fn extracts_stream_from_root_and_expression() {
        assert_eq!(
            extract_stream(
                &table(),
                &[col(STREAM_COLUMN)
                    .eq(lit(42_i64))
                    .and(col("value").gt(lit(10_i64)))]
            )
            .expect("stream extraction succeeds"),
            42
        );
    }

    #[test]
    fn rejects_missing_stream_filter() {
        assert!(extract_stream(&table(), &[col("value").gt(lit(10_i64))]).is_err());
    }

    #[test]
    fn rejects_non_equality_stream_filter() {
        assert!(extract_stream(&table(), &[col(STREAM_COLUMN).gt(lit(42_i64))]).is_err());
    }

    #[test]
    fn rejects_non_int64_stream_value() {
        assert!(extract_stream(&table(), &[col(STREAM_COLUMN).eq(lit("42"))]).is_err());
    }

    #[test]
    fn rejects_stream_under_or_expression() {
        assert!(
            extract_stream(
                &table(),
                &[col(STREAM_COLUMN)
                    .eq(lit(42_i64))
                    .and(col("value").gt(lit(10_i64)))
                    .or(col(STREAM_COLUMN)
                        .eq(lit(42_i64))
                        .and(col("value").lt(lit(20_i64))))]
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_conflicting_stream_filters() {
        assert!(
            extract_stream(
                &table(),
                &[
                    col(STREAM_COLUMN).eq(lit(42_i64)),
                    col(STREAM_COLUMN).eq(lit(43_i64)),
                ],
            )
            .is_err()
        );
    }
}
