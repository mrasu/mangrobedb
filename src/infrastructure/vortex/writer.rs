use std::fs::{self, File};

use crate::domain::file_batch::VortexFileRecord;
use crate::domain::statistics::FileStatistics;
use anyhow::Context;
use tempfile::NamedTempFile;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

const TEMP_SUBDIR: &str = "mangrobe-db";

#[derive(Debug)]
pub struct VortexWriteResult {
    pub temp_file: NamedTempFile,
    pub statistics: FileStatistics,
    pub file_size: u64,
}

pub fn write_vortex_file(
    file_record: &VortexFileRecord,
) -> Result<VortexWriteResult, anyhow::Error> {
    let statistics = file_record.calculate_statistics();

    let temp_dir = std::env::temp_dir().join(TEMP_SUBDIR);
    fs::create_dir_all(&temp_dir)?;
    let temp_file = NamedTempFile::new_in(&temp_dir)?;
    let file = File::create(temp_file.path())?;

    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());

    let array = ArrayRef::from_arrow(file_record.batch_record().clone(), false)?;
    let summary = session
        .write_options()
        .blocking(&runtime)
        .write(file, array.to_array_iterator())?;

    Ok(VortexWriteResult {
        temp_file,
        statistics,
        file_size: summary.size(),
    })
}
