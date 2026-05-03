use anyhow::Error;
use std::path::Path;
use std::sync::Arc;

pub trait ObjectStorePort {
    fn upload(
        &self,
        table_name: &str,
        table_relative_path: &str,
        local_temp_path: &Path,
    ) -> Result<(), Error>;
}

impl<T> ObjectStorePort for Arc<T>
where
    T: ObjectStorePort + ?Sized,
{
    fn upload(
        &self,
        table_name: &str,
        table_relative_path: &str,
        local_temp_path: &Path,
    ) -> Result<(), Error> {
        (**self).upload(table_name, table_relative_path, local_temp_path)
    }
}
