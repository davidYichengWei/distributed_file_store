// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileChunk {
    #[prost(string, tag = "1")]
    pub file_name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub chunk_index: u32,
    #[prost(bytes = "vec", tag = "3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub size: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileMetadata {
    #[prost(string, tag = "1")]
    pub file_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub total_size: u64,
    #[prost(message, repeated, tag = "3")]
    pub chunks: ::prost::alloc::vec::Vec<ChunkMetadata>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChunkMetadata {
    #[prost(string, tag = "1")]
    pub file_name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub chunk_index: u32,
    #[prost(string, tag = "3")]
    pub server_address: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub server_port: u32,
    #[prost(string, tag = "5")]
    pub replica_server_address: ::prost::alloc::string::String,
    #[prost(uint32, tag = "6")]
    pub replica_server_port: u32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Status {
    Ok = 0,
    Error = 1,
    Timeout = 2,
}
impl Status {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Status::Ok => "OK",
            Status::Error => "ERROR",
            Status::Timeout => "TIMEOUT",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "OK" => Some(Self::Ok),
            "ERROR" => Some(Self::Error),
            "TIMEOUT" => Some(Self::Timeout),
            _ => None,
        }
    }
}