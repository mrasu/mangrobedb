use crate::domain::table_schema::TableSchema;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TableRepositoryError {
    #[error("table not found: {table_name}")]
    TableNotFound { table_name: String },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub trait TableRepository {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, TableRepositoryError>;

    fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), TableRepositoryError>;
}

impl<T> TableRepository for Arc<T>
where
    T: TableRepository + ?Sized,
{
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, TableRepositoryError> {
        (**self).get_table_schema(table_name)
    }

    fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), TableRepositoryError> {
        (**self).update_table_schema(table_name, schema)
    }
}
