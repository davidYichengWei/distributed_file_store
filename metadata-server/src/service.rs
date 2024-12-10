use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::io;
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
    metadata_store: Arc<Mutex<HashMap<String, String>>>,
}

impl MetadataService {
    /// Creates a new instance of MetadataService
    pub async fn new() -> Result<Self, io::Error> {
        let metadata_store = Arc::new(Mutex::new(HashMap::new()));

        Ok(Self { metadata_store })
    }
}

#[tonic::async_trait]
impl MetadataServer for MetadataService {
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

    async fn add_server(
        &self,
        _request: Request<AddServerRequest>,
    ) -> Result<Response<AddServerResponse>, Status> {
        println!("add_server called");
        Ok(Response::new(AddServerResponse {
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
        _request: Request<ServerHeartbeatRequest>,
    ) -> Result<Response<ServerHeartbeatResponse>, Status> {
        println!("do_server_heartbeat called");

        Ok(Response::new(ServerHeartbeatResponse {
            status: CommonStatus::Ok as i32,
        }))
    }
}
