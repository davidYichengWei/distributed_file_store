use std::{
    collections::HashMap,
    sync::{Arc},
    thread,
    time::Duration,
    io,
    hash::Hasher
};
use tokio::sync::Mutex;
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
    ChunkMetadata,
    FileChunkTarget,
    TransmitFileChunkRequest,
    DeleteFileChunkRequest,
};
use proto::storage_server_client::{
    StorageServerClient,
};

// use metadata_server::config::HEART_BEAT_EXPIRE_SECS;
static HEART_BEAT_EXPIRE_SECS: usize = 5;
static CHUNK_SIZE: usize = 10 * 1024;

#[derive(Debug, Default)]
pub struct MetadataService {
    metadata_store: Arc<Mutex<HashMap<String, FileMetadata>>>,
    hash_ring: Arc<Mutex<HashMap<String, usize>>>,
    heartbeats: Arc<Mutex<HashMap<String, usize>>>,
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

                let mut heartbeats = heartbeats.lock().await;
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
    
        let server_key = format!("{}:{}", server_address, server_port);
        let server_hash = Self::compute_hash(&server_address, server_port);

        // Add server to the hash ring and heartbeats
        {
            let mut hash_ring = self.hash_ring.lock().await;
            if hash_ring.contains_key(&server_key) {
                println!("Server already exists in the hash ring: {}", server_key);
                return Ok(Response::new(AddServerResponse {
                    status: CommonStatus::Ok as i32,
                }));
            }
            hash_ring.insert(server_key.clone(), server_hash);
            println!("Added server to hash ring: {} -> {}", server_key, server_hash);
        }

        {
            let mut heartbeats = self.heartbeats.lock().await;
            heartbeats.insert(server_key.clone(), HEART_BEAT_EXPIRE_SECS);
        }

        // Spawn a background task to rebalance metadata
        let metadata_store = Arc::clone(&self.metadata_store);
        let hash_ring = Arc::clone(&self.hash_ring);
        tokio::spawn(async move {
            if let Err(e) = rebalance_chunks(metadata_store, hash_ring).await {
                eprintln!("Error during metadata rebalance: {}", e);
            }
        });

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
            let metadata_store = self.metadata_store.lock().await;
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
            let hash_ring = self.hash_ring.lock().await;
            let servers: Vec<_> = hash_ring.keys().cloned().collect();
            if servers.is_empty() {
                return Err(Status::failed_precondition("No servers available in the hash ring."));
            }
            servers
        };

        let hash_ring = self.hash_ring.lock().await;
    
        // Generate chunk metadata
        let chunks: Vec<ChunkMetadata> = (0..num_chunks as u32)
            .map(|chunk_index| {
                // Find primary and replica servers
                let primary_server = get_responsible_server(&hash_ring, chunk_index);
                let replica_server = get_next_server(&hash_ring, &primary_server);
    
                let (primary_address, primary_port) = parse_server(&primary_server);
                let (replica_address, replica_port) = parse_server(&replica_server);
    
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
            let mut metadata_store = self.metadata_store.lock().await;
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
            let mut heartbeats = self.heartbeats.lock().await;
            if heartbeats.contains_key(&server_key) {
                heartbeats.insert(server_key.clone(), HEART_BEAT_EXPIRE_SECS);
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

fn get_responsible_server(
    hash_ring: &tokio::sync::MutexGuard<'_, HashMap<std::string::String, usize>>,
    chunk_index: u32,
) -> String {
    let keys = hash_ring.keys().cloned().collect::<Vec<_>>();
    keys[chunk_index as usize % keys.len()].clone()
}

fn get_next_server(
    hash_ring: &tokio::sync::MutexGuard<'_, HashMap<std::string::String, usize>>,
    current_server: &str,
) -> String {
    let keys = hash_ring.keys().cloned().collect::<Vec<_>>();
    let index = keys.iter().position(|x| x == current_server).unwrap();
    keys[(index + 1) % keys.len()].clone()
}



async fn transmit_chunk_to_target(
    source_server: &str,
    targets: Vec<FileChunkTarget>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StorageServerClient::connect(format!("http://{}", source_server)).await?;
    let request = tonic::Request::new(TransmitFileChunkRequest { targets });
    client.transmit_file_chunk(request).await?;
    Ok(())
}

async fn delete_chunk_from_server(
    server: &str,
    chunk: ChunkMetadata,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StorageServerClient::connect(format!("http://{}", server)).await?;
    let request = tonic::Request::new(DeleteFileChunkRequest {
        file_name: chunk.file_name,
        chunk_index: chunk.chunk_index,
    });
    client.delete_file_chunk(request).await?;
    Ok(())
}

fn is_responsible_or_replica(
    hash_ring: &tokio::sync::MutexGuard<'_, HashMap<std::string::String, usize>>,
    chunk: &ChunkMetadata,
) -> bool {
    // Compute new responsible server
    let primary = get_responsible_server(hash_ring, chunk.chunk_index);
    let replica = get_next_server(hash_ring, &primary);

    // Current servers (from chunk metadata)
    let current_primary = format!("{}:{}", chunk.server_address, chunk.server_port);
    let current_replica = format!(
        "{}:{}",
        chunk.replica_server_address, chunk.replica_server_port
    );

    // Check if the chunk belongs to the recomputed responsible servers
    current_primary == primary || current_replica == replica
}



fn get_chunks_on_server(
    metadata_store: HashMap<String, FileMetadata>,
    server: &str,
) -> Vec<ChunkMetadata> {
    metadata_store
        .values()
        .flat_map(|file| file.chunks.iter())
        .filter(|chunk| {
            let primary_server = format!("{}:{}", chunk.server_address, chunk.server_port);
            let replica_server = format!(
                "{}:{}",
                chunk.replica_server_address, chunk.replica_server_port
            );
            primary_server == server || replica_server == server
        })
        .cloned()
        .collect()
}


async fn rebalance_chunks(
    metadata_store: Arc<Mutex<HashMap<String, FileMetadata>>>,
    hash_ring: Arc<Mutex<HashMap<String, usize>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clone the original metadata_store to preserve the old version for delete operations
    let original_metadata_store = {
        let store = metadata_store.lock().await;
        store.clone()
    };

    // Acquire the current hash_ring
    let hash_ring = hash_ring.lock().await;

    // Iterate over the original metadata_store
    for (file_name, file_metadata) in original_metadata_store.iter() {
        for chunk in &file_metadata.chunks {
            // Determine the new responsible servers
            let new_primary = get_responsible_server(&hash_ring, chunk.chunk_index);
            let new_replica = get_next_server(&hash_ring, &new_primary);

            // Current servers from chunk metadata
            let current_server = format!("{}:{}", chunk.server_address, chunk.server_port);
            let current_replica = format!(
                "{}:{}",
                chunk.replica_server_address, chunk.replica_server_port
            );

            // Transmit chunk to the new primary if needed
            if current_server != new_primary {
                let target = FileChunkTarget {
                    file_name: chunk.file_name.clone(),
                    chunk_index: chunk.chunk_index,
                    target_server_ip: new_primary.split(':').next().unwrap().to_string(),
                    target_server_port: new_primary.split(':').nth(1).unwrap().parse().unwrap(),
                };

                if let Err(e) = transmit_chunk_to_target(&current_server, vec![target]).await {
                    println!("Error transmitting chunk to new primary: {}", e);
                }
            }

            // Transmit chunk to the new replica if needed
            if current_replica != new_replica {
                let target = FileChunkTarget {
                    file_name: chunk.file_name.clone(),
                    chunk_index: chunk.chunk_index,
                    target_server_ip: new_replica.split(':').next().unwrap().to_string(),
                    target_server_port: new_replica.split(':').nth(1).unwrap().parse().unwrap(),
                };

                if let Err(e) = transmit_chunk_to_target(&current_server, vec![target]).await {
                    println!("Error transmitting chunk to new replica: {}", e);
                }
            }

            // Update the metadata_store to reflect the new server assignments
            {
                let mut store = metadata_store.lock().await;
                if let Some(file_metadata) = store.get_mut(file_name) {
                    if let Some(chunk_mut) = file_metadata
                        .chunks
                        .iter_mut()
                        .find(|c| c.chunk_index == chunk.chunk_index)
                    {
                        let (new_primary_ip, new_primary_port) = parse_server(&new_primary);
                        chunk_mut.server_address = new_primary_ip;
                        chunk_mut.server_port = new_primary_port;

                        let (new_replica_ip, new_replica_port) = parse_server(&new_replica);
                        chunk_mut.replica_server_address = new_replica_ip;
                        chunk_mut.replica_server_port = new_replica_port;
                    }
                }
            }
        }
    }

    {
        for (server, _) in hash_ring.iter() {
            println!("Server: {}!", server);
            let store = metadata_store.lock().await;
            for (_, file_metadata) in original_metadata_store.iter() {
                // Iterate over all chunks in each file
                for chunk in &file_metadata.chunks {
                    // Determine the current server from the chunk
                    let current_server = format!("{}:{}", chunk.server_address, chunk.server_port);
                    let replica_server = format!(
                        "{}:{}",
                        chunk.replica_server_address, chunk.replica_server_port
                    );
        
        
                    // Check if the replica server is still responsible
                    if !is_chunk_responsible_or_replica_in_updated_store(chunk, &store, &server) {
                        if let Err(e) = delete_chunk_from_server(&server, chunk.clone()).await {
                            println!(
                                "Error deleting chunk from server {}: {}",
                                replica_server, e
                            );
                        }
                    }
                }
            }
        }
    }
    
    

    {
        let store = metadata_store.lock().await;
        println!("Metadata store after rebalancing:");
        for (file_name, file_metadata) in store.iter() {
            println!("File: {}", file_name);
            for chunk in &file_metadata.chunks {
                println!(
                    "  Chunk {} -> Primary: {}:{}, Replica: {}:{}",
                    chunk.chunk_index,
                    chunk.server_address,
                    chunk.server_port,
                    chunk.replica_server_address,
                    chunk.replica_server_port
                );
            }
        }
    }

    Ok(())
}

fn is_chunk_responsible_or_replica_in_updated_store(
    chunk: &ChunkMetadata,
    updated_store: &HashMap<String, FileMetadata>,
    server: &str,
) -> bool {
    if let Some(updated_metadata) = updated_store.get(&chunk.file_name) {
        if let Some(updated_chunk) = updated_metadata
            .chunks
            .iter()
            .find(|c| c.chunk_index == chunk.chunk_index)
        {
            let primary_server = format!("{}:{}", updated_chunk.server_address, updated_chunk.server_port);
            let replica_server = format!(
                "{}:{}",
                updated_chunk.replica_server_address, updated_chunk.replica_server_port
            );
            println!("Server: {} Chunk: {} Primary: {} Replica: {}",
        server, chunk.chunk_index, primary_server, replica_server);
            return primary_server == server || replica_server == server;
        }
    }
    false
}
