use proto::common::{FileMetadata, Status as CommonStatus};
use proto::metadata::metadata_server_server::MetadataServer;
use proto::metadata::{
    AddServerRequest, AddServerResponse, CommitPutRequest, CommitPutResponse,
    DecrementRequestRequest, DecrementRequestResponse, PutMetadataRequest, PutMetadataResponse,
    RequestFileMetadataRequest, RequestFileMetadataResponse, RollbackPutRequest,
    RollbackPutResponse, ServerHeartbeatRequest, ServerHeartbeatResponse,
};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct MetadataService;

impl MetadataService {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self)
    }
}

#[tonic::async_trait]
impl MetadataServer for MetadataService {
    async fn put_metadata(
        &self,
        request: Request<PutMetadataRequest>,
    ) -> Result<Response<PutMetadataResponse>, tonic::Status> {
        println!("Received PutMetadataRequest: {:?}", request);

        // Here you would handle the request and store the metadata
        // For now, we just return a success response

        Ok(tonic::Response::new(PutMetadataResponse { success: true }))
    }

    async fn request_file_metadata(
        &self,
        request: tonic::Request<RequestFileMetadataRequest>,
    ) -> Result<tonic::Response<RequestFileMetadataResponse>, tonic::Status> {
        println!("Received RequestFileMetadataRequest: {:?}", request);

        let request = request.into_inner();

        // Process the operation and filename fields
        match request.operation {
            0 => println!("Operation: GET"),
            1 => println!("Operation: PUT"),
            2 => println!("Operation: DELETE"),
            _ => return Err(Status::invalid_argument("Invalid operation")),
        }

        let filename = request.filename;

        // Here you would handle the request and return the file metadata
        // For now, we just return a success response with dummy data

        Ok(tonic::Response::new(RequestFileMetadataResponse {
            status: CommonStatus::Ok as i32,
            metadata: Some(FileMetadata {
                file_name: filename,
                total_size: 1024,
                chunks: vec![], // Replace with actual chunks
            }), // Replace with actual metadata
        }))
    }

    async fn commit_put(
        &self,
        request: Request<CommitPutRequest>,
    ) -> Result<Response<CommitPutResponse>, tonic::Status> {
        println!("Received CommitPutRequest: {:?}", request);

        // Here you would handle the commit request
        // For now, we just return a success response

        Ok(tonic::Response::new(CommitPutResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn rollback_put(
        &self,
        request: tonic::Request<RollbackPutRequest>,
    ) -> Result<tonic::Response<RollbackPutResponse>, tonic::Status> {
        println!("Received RollbackPutRequest: {:?}", request);

        // Here you would handle the rollback request
        // For now, we just return a success response

        Ok(tonic::Response::new(RollbackPutResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn decrement_client_ongoing_request(
        &self,
        request: tonic::Request<DecrementRequestRequest>,
    ) -> Result<tonic::Response<DecrementRequestResponse>, tonic::Status> {
        println!("Received DecrementRequestRequest: {:?}", request);

        // Here you would handle the decrement request
        // For now, we just return a success response

        Ok(tonic::Response::new(DecrementRequestResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn add_server(
        &self,
        request: tonic::Request<AddServerRequest>,
    ) -> Result<tonic::Response<AddServerResponse>, tonic::Status> {
        println!("Received AddServerRequest: {:?}", request);

        // Here you would handle the add server request
        // For now, we just return a success response

        Ok(tonic::Response::new(AddServerResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    // Dummy implementation for server heartbeat
    async fn do_server_heartbeat(
        &self,
        request: Request<ServerHeartbeatRequest>,
    ) -> Result<Response<ServerHeartbeatResponse>, Status> {
        println!(
            "Received heartbeat from server {}:{}",
            request.get_ref().server_address,
            request.get_ref().server_port
        );

        Ok(Response::new(ServerHeartbeatResponse {
            status: CommonStatus::Ok as i32,
        }))
    }
}
