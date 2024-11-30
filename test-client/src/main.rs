use anyhow::Result;
use clap::{Parser, Subcommand};
use proto::storage::storage_server_client::StorageServerClient;
use proto::storage::{GetFileChunkRequest, PutFileChunkRequest};
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
        chunk_index: u32,
        file_path: PathBuf,
        server_ip: String,
        server_port: u16,
    },
    Get {
        file_name: String,
        chunk_index: u32,
        server_ip: String,
        server_port: u16,
    },
}

async fn connect_to_server(ip: &str, port: u16) -> Result<StorageServerClient<Channel>> {
    let addr = format!("http://{}:{}", ip, port);
    let client = StorageServerClient::connect(addr).await?;
    Ok(client)
}

async fn put_file_chunk(
    mut client: StorageServerClient<Channel>,
    file_name: String,
    chunk_index: u32,
    file_path: PathBuf,
) -> Result<()> {
    // Read file content
    let data = fs::read(&file_path).await?;
    let size = data.len() as u64;

    // Create FileChunk
    let chunk = proto::common::FileChunk {
        file_name,
        chunk_index,
        data,
        size,
    };

    // Send request
    let request = tonic::Request::new(PutFileChunkRequest { chunk: Some(chunk) });

    let response = client.put_file_chunk(request).await?;
    println!("Put response: {:?}", response);
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
            chunk_index,
            file_path,
            server_ip,
            server_port,
        } => {
            let client = connect_to_server(&server_ip, server_port).await?;
            put_file_chunk(client, file_name, chunk_index, file_path).await?;
        }
        Command::Get {
            file_name,
            chunk_index,
            server_ip,
            server_port,
        } => {
            let client = connect_to_server(&server_ip, server_port).await?;
            get_file_chunk(client, file_name, chunk_index).await?;
        }
    }

    Ok(())
}
