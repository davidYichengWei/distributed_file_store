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

#[derive(Debug, Default)]
pub struct MetadataService {
    hash_ring: Arc<Mutex<HashMap<String, usize>>>, // Consistent hash ring
    heartbeats: Arc<Mutex<HashMap<String, usize>>>, // Heartbeat countdowns
}

impl MetadataService {
    pub async fn new() -> Result<Self, std::io::Error> {

        let heartbeats = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        Self::start_heartbeat_thread(Arc::clone(&heartbeats));

        Ok(Self {
            hash_ring: Arc::new(Mutex::new(HashMap::<String, usize>::new())),
            heartbeats,
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
            heartbeats.insert(server_key.clone(), 5); // 5 seconds countdown
        }

        Ok(Response::new(AddServerResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn request_file_metadata(
        &self,
        _request: Request<RequestFileMetadataRequest>,
    ) -> Result<Response<RequestFileMetadataResponse>, Status> {
        println!("request_file_metadata called");
        Ok(Response::new(RequestFileMetadataResponse {
            status: CommonStatus::Ok as i32,
            metadata: None,
        }))
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

    async fn put_metadata(&self, _request: Request<PutMetadataRequest>) ->
    Result<Response<PutMetadataResponse>, Status> {
        Ok(Response::new(PutMetadataResponse {
            success: true,
        }))
    }

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
                heartbeats.insert(server_key.clone(), 5); // Reset heartbeat countdown
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
