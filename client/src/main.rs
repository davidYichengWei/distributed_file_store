use anyhow::Result;
use clap::{Parser, Subcommand};
use proto::storage::storage_server_client::StorageServerClient;
use proto::storage::{GetFileChunkRequest, PutFileChunkRequest};
use proto::metadata::metadata_server_client::MetadataServerClient;
use proto::metadata::{PutMetadataRequest, CommitPutRequest, RollbackPutRequest};
use proto::common::{FileMetadata, FileChunk};
use std::path::PathBuf;
use tokio::fs;
use tonic::transport::Channel;
use tonic::Request;
use proto::RequestFileMetadataRequest;

static CHUNK_SIZE: usize = 10 * 1024;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Put {
        file_name: String,
        file_path: PathBuf,
    },
    Get {
        file_name: String,
        chunk_index: u32,
    },
}

async fn connect_to_server() -> Result<StorageServerClient<Channel>> {
    let addr = "http://127.0.0.1:50051";
    let client = StorageServerClient::connect(addr).await?;
    Ok(client)
}

async fn connect_to_metadata_server() -> Result<MetadataServerClient<Channel>> {
    let addr = "http://127.0.0.1:50052";
    let client = MetadataServerClient::connect(addr).await?;
    Ok(client)
}

async fn put_file_chunk(
    mut metadata_client: MetadataServerClient<Channel>,
    file_name: String,
    file_path: PathBuf,
) -> Result<()> {
    // Read file content
    let data = fs::read(&file_path).await?;
    let size = data.len() as u64;

    // Check if metadata exists
    let metadata_request = Request::new(RequestFileMetadataRequest {
        filename: file_name.clone(),
    });

    let file_metadata = match metadata_client.request_file_metadata(metadata_request).await {
        Ok(response) => {
            let metadata = response.into_inner().metadata;
            if let Some(metadata) = metadata {
                println!("File already exists, using UPDATE operation.");
                metadata
            } else {
                println!("File does not exist, using PUT operation.");
                FileMetadata {
                    file_name: file_name.clone(),
                    total_size: size,
                    chunks: vec![], // Chunks will be filled by MetadataServer
                }
            }
        }
        Err(e) => {
            println!("Failed to fetch metadata: {}", e);
            return Ok(());
        }
    };

    // Send PUT Metadata request
    let put_metadata_request = Request::new(PutMetadataRequest {
        metadata: Some(file_metadata),
    });

    let put_metadata_response = metadata_client.put_metadata(put_metadata_request).await?;
    let file_metadata = match put_metadata_response.into_inner().metadata {
        Some(metadata) => metadata,
        None => {
            println!("MetadataServer failed to calculate metadata.");
            return Ok(());
        }
    };

    // Upload chunks to corresponding storage and replication servers
    let mut ack_received = true;

    for chunk_metadata in file_metadata.chunks {
        // Read the chunk data
        let start = (chunk_metadata.chunk_index as usize) * CHUNK_SIZE;
        let end = std::cmp::min(start + CHUNK_SIZE, data.len());
        let chunk_data = &data[start..end];

        // Create file chunk request
        let file_chunk = FileChunk {
            file_name: chunk_metadata.file_name.clone(),
            chunk_index: chunk_metadata.chunk_index,
            data: chunk_data.to_vec(),
            size: chunk_data.len() as u64,
        };

        // Send to primary storage server
        let primary_request = Request::new(PutFileChunkRequest {
            chunk: Some(file_chunk.clone()),
        });
        let primary_result = send_chunk_to_server(
            &chunk_metadata.server_address,
            chunk_metadata.server_port,
            primary_request,
        )
        .await;

        if primary_result.is_err() {
            println!(
                "Failed to upload chunk {} to primary server.",
                chunk_metadata.chunk_index
            );
            ack_received = false;
        }

        // Send to replica storage server
        let replica_request = Request::new(PutFileChunkRequest {
            chunk: Some(file_chunk.clone()),
        });
        let replica_result = send_chunk_to_server(
            &chunk_metadata.replica_server_address,
            chunk_metadata.replica_server_port,
            replica_request,
        )
        .await;

        if replica_result.is_err() {
            println!(
                "Failed to upload chunk {} to replica server.",
                chunk_metadata.chunk_index
            );
            ack_received = false;
        }
    }

    // Handle commit/rollback logic based on ack_received
    if ack_received {
        let commit_request = tonic::Request::new(CommitPutRequest {
            filename: file_name.clone(),
        });
        metadata_client.commit_put(commit_request).await?;
    } else {
        let rollback_request = tonic::Request::new(RollbackPutRequest {
            filename: file_name.clone(),
        });
        metadata_client.rollback_put(rollback_request).await?;
    }

    Ok(())
}

/// Helper function to connect to a storage server and send chunk data
async fn send_chunk_to_server(
    server_address: &str,
    server_port: u32,
    request: Request<PutFileChunkRequest>,
) -> Result<bool> {
    let server_uri = format!("http://{}:{}", server_address, server_port);
    match StorageServerClient::connect(server_uri).await {
        Ok(mut client) => {
            if let Ok(_) = client.put_file_chunk(request).await {
                Ok(true)
            } else {
                Ok(false)
            }
        }
        Err(e) => {
            println!("Failed to connect to server {}:{}", server_address, server_port);
            Ok(true)
        }
    }
}


async fn get_file_chunk(
    mut client: StorageServerClient<Channel>,
    file_name: String,
    chunk_index: u32,
) -> Result<()> {
    let request = tonic::Request::new(GetFileChunkRequest {
        file_name,
        chunk_index,
    });

    let response = client.get_file_chunk(request).await?;
    println!("Get response: {:?}", response);

    // If chunk was successfully retrieved, you might want to save it
    if let Some(chunk) = response.into_inner().chunk {
        let output_path = format!("retrieved_{}_{}", chunk.file_name, chunk.chunk_index);
        fs::write(&output_path, chunk.data).await?;
        println!("Saved retrieved chunk to {}", output_path);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Command::Put {
            file_name,
            file_path,
        } => {
            let mut metadata_client = connect_to_metadata_server().await?;
            put_file_chunk(metadata_client, file_name, file_path).await?;
        }
        Command::Get {
            file_name,
            chunk_index,
        } => {
            let mut client = connect_to_server().await?;
            get_file_chunk(client, file_name, chunk_index).await?;
        }
    }

    Ok(())
}