use crate::domain::port::catalog::{CatalogError, CatalogFile, CatalogPort};
use crate::domain::table_schema::TableSchema;

#[derive(Debug, Clone)]
pub struct Table {
    pub schema: TableSchema,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        Self { schema }
    }

    // TODO: return cache when acceptable.
    pub async fn load<C: CatalogPort>(
        catalog_port: &C,
        table_name: &str,
    ) -> Result<Self, CatalogError> {
        let schema = catalog_port.get_table_schema(table_name).await?;
        Ok(Self::new(schema))
    }

    pub fn build_path(&self, catalog_file: &CatalogFile) -> String {
        // TODO: make the storage scheme configurable via Table fields instead of hardcoding s3:// here.
        format!(
            "s3://{}/{}/{}/{}",
            self.schema.bucket, self.schema.path_prefix, self.schema.table_name, catalog_file.path
        )
    }
}
