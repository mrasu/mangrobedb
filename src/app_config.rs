use config::{Config, Environment, File, FileFormat};
use serde::{Deserialize, Deserializer};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub s3: S3Config,
    pub database_url: String,
    #[serde(
        default = "default_flush_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub flush_interval: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
}

impl AppConfig {
    pub fn load(config_path: Option<&Path>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(path) = config_path {
            builder = builder.add_source(File::from(path.to_path_buf()).required(true));
        } else {
            let default_config_path = Path::new("mangrobe_db.yaml");
            if default_config_path.exists() {
                builder =
                    builder.add_source(File::new("mangrobe_db", FileFormat::Yaml).required(true));
            }
        }

        builder
            .add_source(
                Environment::with_prefix("MANGROBE_DB")
                    .prefix_separator("_")
                    .separator("__"),
            )
            .build()?
            .try_deserialize()
    }
}

fn default_flush_interval() -> Duration {
    Duration::from_millis(100)
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    humantime::parse_duration(&value).map_err(serde::de::Error::custom)
}
