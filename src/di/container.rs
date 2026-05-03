use crate::application::ports::SharedUuidGeneratorPort;

#[derive(Debug, Clone)]
pub struct Container {
    pub uuid_generator: SharedUuidGeneratorPort,
}

impl Container {
    pub fn new(uuid_generator: SharedUuidGeneratorPort) -> Self {
        Self { uuid_generator }
    }
}
