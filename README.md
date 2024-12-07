# distributed_file_store

A distributed file storage system implemented in Rust using gRPC.

## Building

Build entire project:
```
cargo build
```

Build individual components:
```
cargo build -p metadata-server
cargo build -p storage-server
cargo build -p client
```

## Running

Run individual components:
```
cargo run --bin metadata-server
cargo run --bin storage-server -- [--bind_address <server_ip>] [--port <server_port>] [--metadata_server_address <metadata_server_ip>] [--metadata_server_port <metadata_server_port>]
cargo run --bin client
```
