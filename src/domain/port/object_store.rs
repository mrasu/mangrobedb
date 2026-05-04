use anyhow::Error;
use object_store::ObjectStore;
use std::path::Path;
use std::sync::Arc;

pub trait ObjectStorePort {
    fn upload(
        &self,
        table_name: &str,
        table_relative_path: &str,
        local_temp_path: &Path,
    ) -> Result<(), Error>;

    fn bucket_name(&self) -> &str;

    fn object_store(&self) -> Arc<dyn ObjectStore>;
}
