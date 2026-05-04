use crate::domain::port::catalog::{CatalogFile, CatalogPort, CatalogPortError};
use crate::domain::table_schema::TableSchema;

const S3_PREFIX: &str = "s3://";
const TABLE_STORAGE_PREFIX_ROOT: &str = "mangrobe-db";

#[derive(Debug, Clone)]
pub struct Table {
    schema: TableSchema,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        Self { schema }
    }

    pub fn load<C: CatalogPort>(
        catalog_port: &C,
        table_name: &str,
    ) -> Result<Self, CatalogPortError> {
        let schema = catalog_port.get_table_schema(table_name)?;
        Ok(Self::new(schema))
    }

    pub fn build_path(&self, bucket: &str, catalog_file: &CatalogFile) -> String {
        // TODO: make the storage scheme configurable via Table fields instead of hardcoding s3:// here.
        format!(
            "{S3_PREFIX}{bucket}/{TABLE_STORAGE_PREFIX_ROOT}/{}/{}",
            self.schema.name, catalog_file.path
        )
    }
}
