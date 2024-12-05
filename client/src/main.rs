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
    mut storage_client: StorageServerClient<Channel>,
    file_name: String,
    file_path: PathBuf,
) -> Result<()> {
    // Read file content
    let data = fs::read(&file_path).await?;
    let size = data.len() as u64;

    // Send file metadata to Metadata Server
    let file_metadata = FileMetadata {
        file_name: file_name.clone(),
        total_size: size,
        chunks: vec![],
    };
    let metadata_request = tonic::Request::new(PutMetadataRequest {
        metadata: Some(file_metadata),
    });
    metadata_client.put_metadata(metadata_request).await?;

    // Split file into chunks and send each chunk to the appropriate storage server
    let chunk_size = 10 * 1024; // 10KB chunks
    let mut chunk_index = 0;
    let mut ack_received = true;

    for chunk in data.chunks(chunk_size) {
        let chunk_data = chunk.to_vec();
        let chunk_size = chunk_data.len() as u64;

        // Create FileChunk
        let file_chunk = FileChunk {
            file_name: file_name.clone(),
            chunk_index,
            data: chunk_data,
            size: chunk_size,
        };

        // Send request
        let request = tonic::Request::new(PutFileChunkRequest { chunk: Some(file_chunk) });
        let response = storage_client.put_file_chunk(request).await;

        if response.is_err() {
            ack_received = false;
            break;
        }

        chunk_index += 1;
    }

    // Handle commit/rollback logic
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
            let mut storage_client = connect_to_server().await?;
            put_file_chunk(metadata_client, storage_client, file_name, file_path).await?;
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