use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
    io,
    hash::Hasher
};
use tonic::{Request, Response, Status};
use proto::common::Status as CommonStatus;
use proto::metadata::metadata_server_server::MetadataServer;
use proto::metadata::{
    RequestFileMetadataRequest, RequestFileMetadataResponse, CommitPutRequest, CommitPutResponse,
    RollbackPutRequest, RollbackPutResponse, DecrementRequestRequest, DecrementRequestResponse,
    AddServerRequest, AddServerResponse, PutMetadataRequest,PutMetadataResponse, 
    ServerHeartbeatRequest, ServerHeartbeatResponse
};
use proto::{
    FileMetadata,
    ChunkMetadata
};

// use metadata_server::config::HEART_BEAT_EXPIRE_SECS;
static HEART_BEAT_EXPIRE_SECS: usize = 5;
static CHUNK_SIZE: usize = 10 * 1024;

#[derive(Debug, Default)]
pub struct MetadataService {
    metadata_store: Arc<Mutex<HashMap<String, FileMetadata>>>, 
    hash_ring: Arc<Mutex<HashMap<String, usize>>>, // Consistent hash ring
    heartbeats: Arc<Mutex<HashMap<String, usize>>>, // Heartbeat countdowns
}

impl MetadataService {
    pub async fn new() -> Result<Self, std::io::Error> {

        let hash_ring = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        let heartbeats = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        let metadata_store = Arc::new(Mutex::new(HashMap::<String, FileMetadata>::new()));

        Self::start_heartbeat_thread(Arc::clone(&heartbeats));

        Ok(Self {
            hash_ring,
            heartbeats,
            metadata_store,
        })
    }
    
    /// Compute a consistent hash for the server based on address and port
    fn compute_hash(server_address: &str, server_port: u32) -> usize {
        let key = format!("{}:{}", server_address, server_port);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&key, &mut hasher);
        hasher.finish() as usize
    }

    fn start_heartbeat_thread(heartbeats: Arc<Mutex<HashMap<String, usize>>>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                let mut heartbeats = heartbeats.lock().unwrap();
                let mut expired_servers = Vec::new();

                // Decrement heartbeats and collect expired servers
                for (key, countdown) in heartbeats.iter_mut() {
                    if *countdown > 0 {
                        *countdown -= 1;
                    } else {
                        println!("Server heartbeat expired: {}", key);
                        expired_servers.push(key.clone());
                    }
                }

                // Remove expired servers
                for key in expired_servers {
                    heartbeats.remove(&key);
                }
            }
        });
    }
}

#[tonic::async_trait]
impl MetadataServer for MetadataService {

    async fn add_server(
        &self,
        request: Request<AddServerRequest>,
    ) -> Result<Response<AddServerResponse>, Status> {
        let AddServerRequest {
            server_address,
            server_port,
        } = request.into_inner();

        // Compute a consistent hash for the server
        let server_key = format!("{}:{}", server_address, server_port);
        let server_hash = Self::compute_hash(&server_address, server_port);

        // Add to the hash ring
        {
            let mut hash_ring = self.hash_ring.lock().unwrap();
            if hash_ring.contains_key(&server_key) {
                println!("Server already exists in the hash ring: {}", server_key);
                return Ok(Response::new(AddServerResponse {
                    status: CommonStatus::Ok as i32,
                }));
            }
            hash_ring.insert(server_key.clone(), server_hash);
            println!("Added server to hash ring: {} -> {}", server_key, server_hash);
        }

        // Add to heartbeats with initial value
        {
            let mut heartbeats = self.heartbeats.lock().unwrap();
            heartbeats.insert(server_key.clone(), HEART_BEAT_EXPIRE_SECS); // 5 seconds countdown
        }

        Ok(Response::new(AddServerResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn request_file_metadata(
        &self,
        request: Request<RequestFileMetadataRequest>,
    ) -> Result<Response<RequestFileMetadataResponse>, Status> {
        // Extract file name from the request
        let file_name = request.into_inner().filename;

        // Check if the file metadata exists in the store
        let metadata = {
            let metadata_store = self.metadata_store.lock().unwrap();
            metadata_store.get(&file_name).cloned() // Clone to release lock quickly
        };

        match metadata {
            Some(file_metadata) => {
                println!("Metadata found for file: {}", file_name);
                Ok(Response::new(RequestFileMetadataResponse {
                    status: CommonStatus::Ok as i32,
                    metadata: Some(file_metadata),
                }))
            }
            None => {
                println!("Metadata not found for file: {}", file_name);
                Ok(Response::new(RequestFileMetadataResponse {
                    status: CommonStatus::Error as i32,
                    metadata: None,
                }))
            }
        }
    }

    async fn commit_put(
        &self,
        _request: Request<CommitPutRequest>,
    ) -> Result<Response<CommitPutResponse>, Status> {
        println!("commit_put called");
        Ok(Response::new(CommitPutResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn rollback_put(
        &self,
        _request: Request<RollbackPutRequest>,
    ) -> Result<Response<RollbackPutResponse>, Status> {
        println!("rollback_put called");
        Ok(Response::new(RollbackPutResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn decrement_client_ongoing_request(
        &self,
        _request: Request<DecrementRequestRequest>,
    ) -> Result<Response<DecrementRequestResponse>, Status> {
        println!("decrement_client_ongoing_request called");
        Ok(Response::new(DecrementRequestResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn put_metadata(
        &self,
        request: Request<PutMetadataRequest>,
    ) -> Result<Response<PutMetadataResponse>, Status> {
        let metadata = match request.into_inner().metadata {
            Some(metadata) => metadata,
            None => return Err(Status::invalid_argument("Metadata is missing")),
        };
    
        let file_name = metadata.file_name.clone();
        let total_size = metadata.total_size;
    
        if total_size == 0 {
            return Err(Status::invalid_argument("Total size cannot be zero."));
        }
    
        let num_chunks = (total_size as usize + CHUNK_SIZE - 1) / CHUNK_SIZE;
    
        // Acquire hash ring and ensure servers are available
        let servers = {
            let hash_ring = self.hash_ring.lock().unwrap();
            let servers: Vec<_> = hash_ring.keys().cloned().collect();
            if servers.is_empty() {
                return Err(Status::failed_precondition("No servers available in the hash ring."));
            }
            servers
        };
    
        // Generate chunk metadata
        let chunks: Vec<ChunkMetadata> = (0..num_chunks as u32)
            .map(|chunk_index| {
                // Find primary and replica servers
                let primary_server = &servers[chunk_index as usize % servers.len()];
                let replica_server = &servers[(chunk_index as usize + 1) % servers.len()];
    
                let (primary_address, primary_port) = parse_server(primary_server);
                let (replica_address, replica_port) = parse_server(replica_server);
    
                ChunkMetadata {
                    file_name: file_name.clone(),
                    chunk_index,
                    server_address: primary_address,
                    server_port: primary_port,
                    replica_server_address: replica_address,
                    replica_server_port: replica_port,
                }
            })
            .collect();
    
        // Create FileMetadata object
        let file_metadata = FileMetadata {
            file_name: file_name.clone(),
            total_size,
            chunks,
        };
    
        // Store in metadata_store
        {
            let mut metadata_store = self.metadata_store.lock().unwrap();
            metadata_store.insert(file_name.clone(), file_metadata.clone());
        }
    
        Ok(Response::new(PutMetadataResponse {
            success: true,
            metadata: Some(file_metadata),
        }))    }

    async fn do_server_heartbeat(
        &self,
        request: Request<ServerHeartbeatRequest>,
    ) -> Result<Response<ServerHeartbeatResponse>, Status> {
        let ServerHeartbeatRequest {
            server_address,
            server_port,
        } = request.into_inner();

        // Compute the server key
        let server_key = format!("{}:{}", server_address, server_port);

        // Update the heartbeat map
        {
            let mut heartbeats = self.heartbeats.lock().unwrap();
            if heartbeats.contains_key(&server_key) {
                heartbeats.insert(server_key.clone(), HEART_BEAT_EXPIRE_SECS); // Reset heartbeat countdown
                println!("Heartbeat refreshed for server: {}", server_key);
            } else {
                println!(
                    "Warning: Received heartbeat from unknown server: {}. Ignoring.",
                    server_key
                );
            }
        }

        Ok(Response::new(ServerHeartbeatResponse {
            status: CommonStatus::Ok as i32,
        }))
    }
}


fn parse_server(server: &str) -> (String, u32) {
    let parts: Vec<&str> = server.split(':').collect();
    let address = parts[0].to_string();
    let port = parts[1].parse().expect("Invalid server port format");
    (address, port)
}
