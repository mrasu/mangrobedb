use crate::application::datafusion::query::codec::create_table::CreateTableOptions;
use crate::application::datafusion::query::codec::error::{validation_error, CodecError};
use crate::domain::table_schema::{ExternalLocation, ExternalLocationScheme};
use url::Url;

const S3_SCHEME: &str = "s3";

pub(super) fn to_domain_external_location(
    location: &Option<String>,
    options: &CreateTableOptions,
) -> Result<ExternalLocation, CodecError> {
    let Some(location) = location else {
        return Err(validation_error("location not specified"));
    };

    let (scheme, bucket, prefix) = parse_location_string(location)?;

    Ok(ExternalLocation::new(
        scheme,
        bucket,
        prefix,
        options.endpoint.clone(),
        options.region.clone(),
    ))
}

fn parse_location_string(
    location: &str,
) -> Result<(ExternalLocationScheme, String, String), CodecError> {
    let url = Url::parse(location).map_err(|err| CodecError::InvalidLocation {
        message: err.to_string(),
    })?;
    let scheme = match url.scheme() {
        S3_SCHEME => ExternalLocationScheme::S3,
        _ => {
            return Err(validation_error(format!(
                "only s3 locations are supported: {location}"
            )));
        }
    };

    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| validation_error(format!("s3 bucket is required: {location}")))?;

    let prefix = url.path().trim_start_matches('/').to_string();

    Ok((scheme, bucket.into(), prefix))
}

pub(super) fn from_domain_external_location(location: &ExternalLocation) -> String {
    let scheme = match location.scheme {
        ExternalLocationScheme::S3 => S3_SCHEME,
    };

    if location.prefix.is_empty() {
        format!("{scheme}://{}", location.bucket)
    } else {
        format!("{scheme}://{}/{}", location.bucket, location.prefix)
    }
}
