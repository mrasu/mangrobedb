use crate::domain::port::uuid_generator::UuidGeneratorPort;
use std::sync::Arc;

/// Common, cross-cutting ports (e.g., time, randomness, UUID).
/// Does not include domain-specific dependencies.
#[derive(Debug, Clone)]
pub struct CommonPorts {
    pub uuid_generator: Arc<dyn UuidGeneratorPort + Send + Sync>,
}

impl CommonPorts {
    pub fn new(uuid_generator: Arc<dyn UuidGeneratorPort + Send + Sync>) -> Self {
        Self { uuid_generator }
    }
}
