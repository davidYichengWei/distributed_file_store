use proto::common::Status as CommonStatus;
use proto::storage::storage_server_client::StorageServerClient;
use proto::storage::storage_server_server::StorageServer;
use proto::storage::{
    DeleteFileChunkRequest, DeleteFileChunkResponse, FailedFileChunk, GetFileChunkRequest,
    GetFileChunkResponse, PutFileChunkRequest, PutFileChunkResponse, RollbackFileChunkRequest,
    RollbackFileChunkResponse, TransmitFileChunkRequest, TransmitFileChunkResponse,
};
use std::path::PathBuf;
use tokio::fs;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct StorageService {
    data_dir: PathBuf,
}

impl StorageService {
    pub async fn new(port: u16) -> Result<Self, std::io::Error> {
        let data_dir = PathBuf::from(format!("./data/{}", port));
        // Create data directory if it doesn't exist
        fs::create_dir_all(&data_dir).await?;
        Ok(Self { data_dir })
    }

    fn get_chunk_path(&self, file_name: &str, chunk_index: u32) -> PathBuf {
        self.data_dir.join(format!("{}_{}", file_name, chunk_index))
    }
}

#[tonic::async_trait]
impl StorageServer for StorageService {
    async fn put_file_chunk(
        &self,
        request: Request<PutFileChunkRequest>,
    ) -> Result<Response<PutFileChunkResponse>, Status> {
        let chunk = request
            .into_inner()
            .chunk
            .ok_or_else(|| Status::invalid_argument("Missing chunk data in request"))?;

        let chunk_path = self.get_chunk_path(&chunk.file_name, chunk.chunk_index);

        // Write chunk data to file
        if let Err(e) = fs::write(&chunk_path, chunk.data).await {
            println!("Failed to write chunk: {}", e);
            return Ok(Response::new(PutFileChunkResponse {
                status: CommonStatus::Error as i32,
            }));
        }

        println!("Successfully stored chunk at {:?}", chunk_path);
        Ok(Response::new(PutFileChunkResponse {
            status: CommonStatus::Ok as i32,
        }))
    }

    async fn delete_file_chunk(
        &self,
        request: Request<DeleteFileChunkRequest>,
    ) -> Result<Response<DeleteFileChunkResponse>, Status> {
        let request = request.into_inner();
        let chunk_path = self.get_chunk_path(&request.file_name, request.chunk_index);

        match fs::remove_file(&chunk_path).await {
            Ok(_) => {
                println!("Successfully deleted chunk at {:?}", chunk_path);
                Ok(Response::new(DeleteFileChunkResponse {
                    status: CommonStatus::Ok as i32,
                }))
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    println!("Chunk not found at {:?}", chunk_path);
                    Ok(Response::new(DeleteFileChunkResponse {
                        status: CommonStatus::Error as i32,
                    }))
                }
                _ => {
                    println!("Failed to delete chunk: {}", e);
                    Ok(Response::new(DeleteFileChunkResponse {
                        status: CommonStatus::Error as i32,
                    }))
                }
            },
        }
    }

    async fn rollback_file_chunk(
        &self,
        request: Request<RollbackFileChunkRequest>,
    ) -> Result<Response<RollbackFileChunkResponse>, Status> {
        let request = request.into_inner();
        let chunk_path = self.get_chunk_path(&request.file_name, request.chunk_index);

        match fs::remove_file(&chunk_path).await {
            Ok(_) => {
                println!(
                    "Successfully rolled back (deleted) chunk at {:?}",
                    chunk_path
                );
                Ok(Response::new(RollbackFileChunkResponse {
                    status: CommonStatus::Ok as i32,
                }))
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    println!("Chunk not found at {:?}", chunk_path);
                    Ok(Response::new(RollbackFileChunkResponse {
                        status: CommonStatus::Error as i32,
                    }))
                }
                _ => {
                    println!("Failed to rollback chunk: {}", e);
                    Ok(Response::new(RollbackFileChunkResponse {
                        status: CommonStatus::Error as i32,
                    }))
                }
            },
        }
    }

    async fn get_file_chunk(
        &self,
        request: Request<GetFileChunkRequest>,
    ) -> Result<Response<GetFileChunkResponse>, Status> {
        let request = request.into_inner();
        let chunk_path = self.get_chunk_path(&request.file_name, request.chunk_index);

        // Read chunk data from file
        let data = match fs::read(&chunk_path).await {
            Ok(data) => data,
            Err(e) => {
                println!("Failed to read chunk: {}", e);
                return Ok(Response::new(GetFileChunkResponse {
                    status: CommonStatus::Error as i32,
                    chunk: None,
                }));
            }
        };

        // Get the size before moving data
        let size = data.len() as u64;

        // Create FileChunk response
        let chunk = proto::common::FileChunk {
            file_name: request.file_name,
            chunk_index: request.chunk_index,
            data, // data is moved here
            size, // we use the pre-calculated size
        };

        println!("Successfully retrieved chunk from {:?}", chunk_path);
        Ok(Response::new(GetFileChunkResponse {
            status: CommonStatus::Ok as i32,
            chunk: Some(chunk),
        }))
    }

    async fn transmit_file_chunk(
        &self,
        request: Request<TransmitFileChunkRequest>,
    ) -> Result<Response<TransmitFileChunkResponse>, Status> {
        let request = request.into_inner();
        let mut failed_chunks: Vec<FailedFileChunk> = Vec::new();

        for target in request.targets {
            let chunk_path = self.get_chunk_path(&target.file_name, target.chunk_index);

            // Try to read the chunk
            let data = match fs::read(&chunk_path).await {
                Ok(data) => data,
                Err(e) => {
                    println!("Failed to read chunk at {:?}: {}", chunk_path, e);
                    failed_chunks.push(FailedFileChunk {
                        file_name: target.file_name.clone(),
                        chunk_index: target.chunk_index,
                    });
                    continue;
                }
            };

            // Create the chunk object
            let data_size = data.len();
            let chunk = proto::common::FileChunk {
                file_name: target.file_name.clone(),
                chunk_index: target.chunk_index,
                data,
                size: data_size as u64,
            };

            // Create client and attempt to send chunk
            let target_addr = format!(
                "http://{}:{}",
                target.target_server_ip, target.target_server_port
            );
            match StorageServerClient::connect(target_addr).await {
                Ok(mut client) => {
                    let put_request = Request::new(PutFileChunkRequest { chunk: Some(chunk) });

                    match client.put_file_chunk(put_request).await {
                        Ok(response) => {
                            if response.into_inner().status != CommonStatus::Ok as i32 {
                                println!("Failed to put chunk on target server");
                                failed_chunks.push(FailedFileChunk {
                                    file_name: target.file_name,
                                    chunk_index: target.chunk_index,
                                });
                            }
                        }
                        Err(e) => {
                            println!("Error putting chunk on target server: {}", e);
                            failed_chunks.push(FailedFileChunk {
                                file_name: target.file_name,
                                chunk_index: target.chunk_index,
                            });
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to connect to target server: {}", e);
                    failed_chunks.push(FailedFileChunk {
                        file_name: target.file_name,
                        chunk_index: target.chunk_index,
                    });
                }
            }
        }

        // Return response with any failed chunks
        Ok(Response::new(TransmitFileChunkResponse {
            status: if failed_chunks.is_empty() {
                CommonStatus::Ok as i32
            } else {
                CommonStatus::Error as i32
            },
            failed_chunks,
        }))
    }
}
