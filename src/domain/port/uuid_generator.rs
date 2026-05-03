use std::fmt::Debug;
use uuid::Uuid;

pub trait UuidGeneratorPort: Debug {
    fn generate(&self) -> Uuid;
}
