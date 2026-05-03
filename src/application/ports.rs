use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

pub type SharedUuidGeneratorPort = Arc<dyn UuidGeneratorPort + Send + Sync>;

pub trait UuidGeneratorPort: Debug {
    fn generate(&self) -> Uuid;
}
