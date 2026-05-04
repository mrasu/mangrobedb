use anyhow::Error;
use std::path::Path;

pub trait ObjectStorePort {
    fn upload(
        &self,
        table_name: &str,
        table_relative_path: &str,
        local_temp_path: &Path,
    ) -> Result<(), Error>;
}
