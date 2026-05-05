use std::collections::HashSet;

use crate::application::datafusion::column::INTERNAL_COLUMN_PREFIX;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::Statement;
use datafusion_expr::utils::expr_to_columns;

pub async fn execute_sql(
    ctx: &SessionContext,
    sql: &str,
    allowed_tables: &[&str],
) -> Result<DataFrame, DataFusionError> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;

    let statement = state.sql_to_statement(sql, &dialect)?;
    validate_statement(&state, &statement, allowed_tables)?;

    let plan = state.statement_to_plan(statement).await?;
    validate_plan(&plan)?;

    ctx.execute_logical_plan(plan).await
}

fn validate_statement(
    state: &SessionState,
    statement: &Statement,
    allowed_tables: &[&str],
) -> Result<(), DataFusionError> {
    validate_statement_kind(statement)?;
    validate_table_references(state, statement, allowed_tables)
}

fn validate_statement_kind(statement: &Statement) -> Result<(), DataFusionError> {
    match statement {
        Statement::Statement(statement) => match statement.as_ref() {
            datafusion::sql::sqlparser::ast::Statement::Query(_) => Ok(()),
            _ => Err(DataFusionError::Plan(
                "only SELECT queries are supported".to_string(),
            )),
        },
        _ => Err(DataFusionError::Plan(
            "only SELECT queries are supported".to_string(),
        )),
    }
}

fn validate_table_references(
    state: &SessionState,
    statement: &Statement,
    allowed_tables: &[&str],
) -> Result<(), DataFusionError> {
    let allowed_tables: HashSet<_> = allowed_tables.iter().copied().collect();
    let references = state.resolve_table_references(statement)?;

    for reference in references {
        if !allowed_tables.contains(reference.table()) {
            return Err(DataFusionError::Plan(format!(
                "query references a table that is not allowed: {}",
                reference.table()
            )));
        }
    }

    Ok(())
}

fn validate_plan(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    let mut columns = HashSet::new();

    collect_plan_columns(plan, &mut columns)?;

    for column in columns {
        if column.name.starts_with(INTERNAL_COLUMN_PREFIX) {
            return Err(DataFusionError::Plan(format!(
                "query references an internal column that is not allowed: {}",
                column.name
            )));
        }
    }

    Ok(())
}

fn collect_plan_columns(
    plan: &LogicalPlan,
    columns: &mut HashSet<datafusion::common::Column>,
) -> Result<(), DataFusionError> {
    for expr in plan.expressions() {
        expr_to_columns(&expr, columns)?;
    }

    for input in plan.inputs() {
        collect_plan_columns(input, columns)?;
    }

    Ok(())
}
