use crate::domain::port::object_store::ObjectStorePort;
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::block_in_place;
use tracing::info;

const TABLE_STORAGE_PREFIX_ROOT: &str = "mangrobe-db";

#[derive(Debug)]
pub struct S3ObjectStorePort {
    store: Arc<object_store::aws::AmazonS3>,
}

impl S3ObjectStorePort {
    pub fn from_env(bucket: &str) -> Result<Self, anyhow::Error> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()?;
        Ok(Self {
            store: Arc::new(store),
        })
    }
}

impl ObjectStorePort for S3ObjectStorePort {
    fn upload(
        &self,
        table_name: &str,
        table_relative_path: &str,
        local_temp_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let payload = std::fs::read(local_temp_path)?;
        let location = ObjectPath::from(format!(
            "{TABLE_STORAGE_PREFIX_ROOT}/{table_name}/{table_relative_path}"
        ));
        info!(
            table_name,
            table_relative_path,
            local_temp_path = %local_temp_path.display(),
            object_key = %location,
            bytes = payload.len(),
            "uploading file to object store"
        );
        block_in_place(|| Handle::current().block_on(self.store.put(&location, payload.into())))?;
        info!(object_key = %location, "uploaded file to object store");
        Ok(())
    }
}
