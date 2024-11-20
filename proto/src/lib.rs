// Proto crate that organizes and exposes the gRPC service definitions generated from .proto files.
// Includes three core services: common types, storage operations, and metadata management.

pub mod common {
    include!("generated/common.rs");
}

pub mod storage {
    include!("generated/storage.rs");
}

pub mod metadata {
    include!("generated/metadata.rs");
}

pub use common::*;
pub use metadata::*;
pub use storage::*;
