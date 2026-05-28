use crate::application::datafusion::query::codec::error::CodecError;
use crate::domain::file::FileFormat;

pub(super) fn to_domain_file_format(format: &str) -> Result<FileFormat, CodecError> {
    if format.eq_ignore_ascii_case("vortex") {
        Ok(FileFormat::Vortex)
    } else {
        Err(CodecError::NotImplemented {
            message: "Only VORTEX format is supported".into(),
        })
    }
}

pub(super) fn from_domain_file_format(format: &FileFormat) -> &'static str {
    match format {
        FileFormat::Vortex => "vortex",
    }
}
