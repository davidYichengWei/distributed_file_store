# Distributed File Storage System

Yicheng Wei 1004750254 yicheng.wei@mail.utoronto.ca \
Yifan Qu 1005354894 \
Xuhui Chen 1005684537

## Motivation
The motivation for this project arises from the need for reliable, efficient storage solutions in distributed computing environments, where data must be managed across multiple nodes, such as virtual machines or containers. As applications scale, they require storage architectures that are not only robust but also easy to deploy and operate, making a distributed file storage system an invaluable tool.

Rust’s performance, memory safety, and concurrency model make it an ideal language for building such a system. However, the Rust ecosystem currently lacks a comprehensive, user-friendly solution for distributed file storage that includes essential features like node discovery, file chunking, and redundancy. Most Rust-based storage systems are either limited in scope or require significant configuration, limiting their accessibility and ease of use.

Our project aims to fill this gap by developing a modular, Rust-native distributed file storage system that simplifies the complexities of distributed storage management. By including core functionalities like node health monitoring, data distribution, and redundancy, and by providing a command-line interface (CLI) for easy operation, we aim to deliver a solution that is both resilient and accessible. This project will contribute a ready-to-use, reliable storage solution to the Rust ecosystem, designed for scalability, ease of deployment, and efficient handling of large datasets across distributed environments.

## Objectives and Key Features
### Objectives:
Our goal is to implement a distributed file system with the following objectives:
1. Persistent storage: The file stored in the system is persistent. 
2. Balance of workload: We use a hash ring technique so that the workload on each connected storage server is approximately the same.
3. Fault tolarance: We have 1 replica for each file chunk, so that the system can tolarant 1 server failure simultaneously.
4. Concurrency: The system can handle clients requests concurrently as there are many storage servers in the systems.

### Key Features
Upload files: The client will be able to upload a file to the distributed file system.

Download files: The client will be able to download a file from the system.

Add servers: The system allows for adding storage servers to the system at any time while the system is running.

Remove servers: The system allows for removing storage servers to the system at any time while the system is running. File chunks storaged on the server will still present in the system after the server is removed.

Detect and recover storage server failures: The system can detect the failure of a storage server, and restore the file chunks on the failed server by checking the replicas of the file chunks.


## User's Guide
1. Start the metadata server.
2. Start the desired number of storage servers.
3. Use the client to upload, download files.

## Reproducibility Guide
### Building

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

### Running

Run individual components:
```
cargo run --bin metadata-server
cargo run --bin storage-server -- [--bind_address <server_ip>] [--port <server_port>] [--metadata_server_address <metadata_server_ip>] [--metadata_server_port <metadata_server_port>]
cargo run --bin client
```

## Contributions
Metadata Server: Xuhui Chen \
Client: Yifan Qu \
Storage Server: Yicheng Wei

## Lessons Learned and Remarks
### Rust's Strict Error Handling Mechanism
One of the key advantages we discovered while building this distributed system was Rust's strict error handling mechanism. In a distributed system, various types of failures are inevitable - from network timeouts to server crashes and connection errors. Rust's Result type and pattern matching forced us to explicitly handle these error cases, making our system more robust and reliable. For instance, when implementing the storage server's registration with the metadata server, Rust compelled us to handle not just successful registration but also various failure scenarios like connection timeouts and server rejections. This enforcement of comprehensive error handling helped prevent subtle bugs that could have emerged in production.

### Asynchronous Programming
The adoption of asynchronous programming with Tokio proved to be crucial for system performance. Tokio's async/await syntax provided an intuitive way to handle concurrent operations without the complexity of manual thread management. This was particularly evident in our implementation of the storage server's heartbeat mechanism, where we needed to periodically communicate with the metadata server while simultaneously handling client requests. Tokio's runtime allowed us to spawn these tasks in the background efficiently, ensuring that our system could handle multiple operations concurrently without blocking.

### gRPC and Modular Design
Our decision to use gRPC and adopt a modular design significantly improved the development process. By breaking down the system into three main components - metadata server, storage server, and client - and defining clear gRPC interfaces between them, we created natural boundaries for parallel development. This modular approach allowed team members to work independently on different components while ensuring compatibility through well-defined protocol buffer specifications. For example, once we settled on the storage server's interface for file chunk operations, team members could develop the client and server implementations independently, leading to more efficient collaboration and easier testing.