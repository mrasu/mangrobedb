use crate::domain::table::Table;
use anyhow::Error;
use object_store::ObjectStore;
use std::path::Path;
use std::sync::Arc;

pub trait ObjectStorePort {
    fn upload(
        &self,
        table: &Table,
        table_relative_path: &str,
        local_temp_path: &Path,
    ) -> Result<(), Error>;

    // TODO: remove. After accessible from anywhere?
    fn is_accessible(&self, bucket: &str) -> bool;

    fn object_store(&self) -> Arc<dyn ObjectStore>;
}
