use crate::domain::port::object_store::ObjectStorePort;
use crate::domain::table::Table;
use anyhow::anyhow;
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::block_in_place;
use tracing::info;

#[derive(Debug)]
pub struct S3ObjectStore {
    bucket: String,
    store: Arc<object_store::aws::AmazonS3>,
}

impl S3ObjectStore {
    pub fn from_env(bucket: &str) -> Result<Self, anyhow::Error> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()?;
        Ok(Self {
            bucket: bucket.to_string(),
            store: Arc::new(store),
        })
    }
}

impl ObjectStorePort for S3ObjectStore {
    fn upload(
        &self,
        table: &Table,
        table_relative_path: &str,
        local_temp_path: &Path,
    ) -> Result<(), anyhow::Error> {
        self.assert_accessible(&table.schema.bucket)?;

        let payload = std::fs::read(local_temp_path)?;
        let location = table.build_object_path(table_relative_path);
        info!(
            table_name = table.schema.table_name,
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

    fn is_accessible(&self, bucket: &str) -> bool {
        self.bucket == bucket
    }

    fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        Arc::clone(&self.store) as Arc<dyn object_store::ObjectStore>
    }
}

impl S3ObjectStore {
    fn assert_accessible(&self, bucket: &str) -> Result<(), anyhow::Error> {
        if self.is_accessible(bucket) {
            Ok(())
        } else {
            Err(anyhow!("Not accessible bucket: {bucket}"))
        }
    }
}
