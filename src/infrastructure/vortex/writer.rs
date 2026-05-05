use crate::domain::file_batch::VortexFileRecord;
use crate::domain::statistics::FileStatistics;
use async_fs::{File, create_dir_all};
use tempfile::NamedTempFile;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;

const TEMP_SUBDIR: &str = "mangrobe-db";

#[derive(Debug)]
pub struct VortexWriteResult {
    pub temp_file: NamedTempFile,
    pub statistics: FileStatistics,
    pub file_size: u64,
}

pub async fn write_vortex_file(
    file_record: &VortexFileRecord,
) -> Result<VortexWriteResult, anyhow::Error> {
    let statistics = file_record.calculate_statistics();

    let temp_dir = std::env::temp_dir().join(TEMP_SUBDIR);
    create_dir_all(&temp_dir).await?;
    let temp_file = NamedTempFile::new_in(&temp_dir)?;
    let file = File::create(temp_file.path()).await?;

    let session = VortexSession::default();
    let array = ArrayRef::from_arrow(file_record.batch_record().clone(), false)?;
    let summary = session
        .write_options()
        .write(file, array.to_array_stream())
        .await?;

    Ok(VortexWriteResult {
        temp_file,
        statistics,
        file_size: summary.size(),
    })
}
