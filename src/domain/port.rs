use crate::domain::table_schema::TableSchema;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogPortError {
    #[error("table not found: {table_name}")]
    TableNotFound { table_name: String },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub trait CatalogPort {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogPortError>;

    fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogPortError>;
}

impl<T> CatalogPort for Arc<T>
where
    T: CatalogPort + ?Sized,
{
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogPortError> {
        (**self).get_table_schema(table_name)
    }

    fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogPortError> {
        (**self).update_table_schema(table_name, schema)
    }
}
