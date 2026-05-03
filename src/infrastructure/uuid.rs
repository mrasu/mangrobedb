use crate::domain::port::uuid_generator::UuidGeneratorPort;
use uuid::Uuid;

#[derive(Debug)]
pub struct RandomUuid;

impl UuidGeneratorPort for RandomUuid {
    fn generate(&self) -> Uuid {
        Uuid::new_v4()
    }
}
