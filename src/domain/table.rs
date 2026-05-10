use crate::domain::port::catalog::{CatalogError, CatalogFile, CatalogPort};
use crate::domain::table_schema::TableSchema;
use object_store::path::Path as ObjectPath;

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

    pub fn build_full_path(&self, catalog_file: &CatalogFile) -> String {
        format!(
            "s3://{}/{}/{}",
            self.schema.bucket, self.schema.path_prefix, catalog_file.path
        )
    }

    pub fn build_object_path(&self, path: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{}", self.schema.path_prefix, path))
    }
}
