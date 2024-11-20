# distributed_file_store

A distributed file storage system implemented in Rust using gRPC.

## Building

Build entire project:
```
cargo build
```

Build individual components:
```
cargo build -p metadata_server
cargo build -p storage_server
cargo build -p client
```

## Running

Run individual components:
```
cargo run --bin metadata-server
cargo run --bin storage-server
cargo run --bin client
```
