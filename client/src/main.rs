use anyhow::Result;
use clap::{Parser, Subcommand};
use proto::storage::storage_server_client::StorageServerClient;
use proto::storage::{GetFileChunkRequest, PutFileChunkRequest, DeleteFileChunkRequest};
use proto::metadata::metadata_server_client::MetadataServerClient;
use proto::metadata::{PutMetadataRequest, CommitPutRequest, RollbackPutRequest, CommitDeleteRequest, RollbackDeleteRequest};
use proto::common::{FileMetadata, FileChunk};
use std::path::PathBuf;
use tokio::fs;
use tonic::transport::Channel;
use tonic::Request;
use proto::RequestFileMetadataRequest;
use proto::metadata::{RegisterUserRequest, LoginUserRequest, LoginUserResponse};
use std::io::{self, Write};

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
        username: String,
    },
    Get {
        file_name: String,
    },
    Delete {
        file_name: String,
        username: String,
    },
    Register {
        username: String,
        password: String,
    },
    Login {
        username: String,
        password: String,
    },
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
    username: String,
) -> Result<()> {
    // Read file content
    let data = fs::read(&file_path).await?;
    let size = data.len() as u64;

    let owner = username; // Replace with actual username

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
                    owner: owner.clone(), // Add owner field
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
        Err(_e) => {
            println!("Failed to connect to server {}:{}", server_address, server_port);
            Ok(true)
        }
    }
}
async fn delete_file(
    mut metadata_client: MetadataServerClient<Channel>,
    file_name: String,
    username: String,
) -> Result<()> {
    // Step 1: Request file metadata
    let metadata_request = Request::new(RequestFileMetadataRequest {
        filename: file_name.clone(),
    });

    let file_metadata = match metadata_client.request_file_metadata(metadata_request).await {
        Ok(response) => {
            let metadata = response.into_inner().metadata;
            if let Some(metadata) = metadata {
                if metadata.owner != username {
                    println!("You do not have permission to delete this file.");
                    return Ok(());
                }
                metadata
            } else {
                println!("File does not exist.");
                return Ok(());
            }
        }
        Err(e) => {
            println!("Failed to fetch metadata: {}", e);
            return Ok(());
        }
    };

    println!("Retrieved metadata: {:?}", file_metadata);

    // Step 2: Send delete metadata request
    // let delete_metadata_request = Request::new(RequestFileMetadataRequest {
    //     filename: file_name.clone(),
    // });

    // let delete_metadata_response = metadata_client.delete_metadata(delete_metadata_request).await?;
    // let chunk_list = delete_metadata_response.into_inner().chunks;

    let mut ack_received = true;

    // Step 3: Delete chunks from storage servers
    for chunk_metadata in file_metadata.chunks {
        // Delete from primary storage server
        let primary_request = Request::new(DeleteFileChunkRequest {
            file_name: chunk_metadata.file_name.clone(),
            chunk_index: chunk_metadata.chunk_index,
        });
        let primary_result = delete_chunk_from_server(
            &chunk_metadata.server_address,
            chunk_metadata.server_port,
            primary_request,
        )
        .await;

        if primary_result.is_err() {
            println!(
                "Failed to delete chunk {} from primary server.",
                chunk_metadata.chunk_index
            );
            ack_received = false;
        }

        // Delete from replica storage server
        let replica_request = Request::new(DeleteFileChunkRequest {
            file_name: chunk_metadata.file_name.clone(),
            chunk_index: chunk_metadata.chunk_index,
        });
        let replica_result = delete_chunk_from_server(
            &chunk_metadata.replica_server_address,
            chunk_metadata.replica_server_port,
            replica_request,
        )
        .await;

        if replica_result.is_err() {
            println!(
                "Failed to delete chunk {} from replica server.",
                chunk_metadata.chunk_index
            );
            ack_received = false;
        }
    }

    // Step 4: Handle commit or rollback
    if ack_received {
        let commit_request = tonic::Request::new(CommitDeleteRequest {
            filename: file_name.clone(),
            username: username.clone(),
        });
        metadata_client.commit_delete(commit_request).await?;
    } else {
        let rollback_request = tonic::Request::new(RollbackDeleteRequest {
            filename: file_name.clone(),
        });
        metadata_client.rollback_delete(rollback_request).await?;
    }

    Ok(())
}

// Helper function to delete a chunk from a storage server
async fn delete_chunk_from_server(
    server_address: &str,
    server_port: u32,
    request: Request<DeleteFileChunkRequest>,
) -> Result<bool> {
    let server_uri = format!("http://{}:{}", server_address, server_port);
    match StorageServerClient::connect(server_uri).await {
        Ok(mut client) => {
            if client.delete_file_chunk(request).await.is_ok() {
                Ok(true)
            } else {
                Ok(false)
            }
        }
        Err(_) => {
            println!("Failed to connect to server {}:{}", server_address, server_port);
            Ok(false)
        }
    }
}

async fn get_file(
    mut metadata_client: MetadataServerClient<Channel>,
    file_name: String,
) -> Result<()> {
    let metadata_request = Request::new(RequestFileMetadataRequest {
        filename: file_name.clone(),
    });

    let file_metadata = match metadata_client.request_file_metadata(metadata_request).await {
        Ok(response) => {
            let metadata = response.into_inner().metadata;
            if let Some(metadata) = metadata {
                metadata
            } else {
                println!("File does not exist.");
                return Ok(());
            }
        }
        Err(e) => {
            println!("Failed to fetch metadata: {}", e);
            return Ok(());
        }
    };

    println!("Retrieved metadata: {:?}", file_metadata);

    // Step 2: Set up the output path
    let output_path = format!("retrieved_files/{}", file_name);
    let mut assembled_file = Vec::new();

    // Step 3: Retrieve each chunk
    for chunk_metadata in file_metadata.chunks {
        // Connect to the primary server
        let primary_server_uri = format!("http://{}:{}", chunk_metadata.server_address, chunk_metadata.server_port);
        let mut client = StorageServerClient::connect(primary_server_uri).await?;

        // Request the chunk
        let request = tonic::Request::new(GetFileChunkRequest {
            file_name: chunk_metadata.file_name.clone(),
            chunk_index: chunk_metadata.chunk_index,
        });

        match client.get_file_chunk(request).await {
            Ok(response) => {
                if let Some(chunk) = response.into_inner().chunk {
                    assembled_file.extend(chunk.data);
                    println!(
                        "Retrieved chunk {} from {}:{}",
                        chunk.chunk_index, chunk_metadata.server_address, chunk_metadata.server_port
                    );
                } else {
                    eprintln!("Chunk {} missing in response", chunk_metadata.chunk_index);
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to retrieve chunk {} from {}:{} - {}",
                    chunk_metadata.chunk_index, chunk_metadata.server_address, chunk_metadata.server_port, e
                );

                // Retry on the replica server
                let replica_server_uri = format!(
                    "http://{}:{}",
                    chunk_metadata.replica_server_address, chunk_metadata.replica_server_port
                );
                let mut replica_client = StorageServerClient::connect(replica_server_uri).await?;

                let replica_request = tonic::Request::new(GetFileChunkRequest {
                    file_name: chunk_metadata.file_name.clone(),
                    chunk_index: chunk_metadata.chunk_index,
                });

                match replica_client.get_file_chunk(replica_request).await {
                    Ok(replica_response) => {
                        if let Some(chunk) = replica_response.into_inner().chunk {
                            assembled_file.extend(chunk.data);
                            println!(
                                "Retrieved chunk {} from replica {}:{}",
                                chunk.chunk_index, chunk_metadata.replica_server_address, chunk_metadata.replica_server_port
                            );
                        } else {
                            eprintln!("Chunk {} missing in replica response", chunk_metadata.chunk_index);
                        }
                    }
                    Err(replica_err) => {
                        eprintln!(
                            "Failed to retrieve chunk {} from replica {}:{} - {}",
                            chunk_metadata.chunk_index, chunk_metadata.replica_server_address, chunk_metadata.replica_server_port, replica_err
                        );
                    }
                }
            }
        }
    }

    // Step 4: Write the assembled file
    fs::create_dir_all("retrieved_files").await?; // Ensure the directory exists
    fs::write(&output_path, assembled_file).await?;
    println!("File assembled and saved to {}", output_path);

    Ok(())
}

async fn register_user(
    mut metadata_client: MetadataServerClient<Channel>,
    username: String,
    password: String,
) -> Result<()> {
    let request = Request::new(RegisterUserRequest { username, password });
    let response = metadata_client.register_user(request).await?;
    let message = response.into_inner().message;
    println!("Register response: {}", message);
    Ok(())
}

async fn login_user(
    mut metadata_client: MetadataServerClient<Channel>,
    username: String,
    password: String,
) -> Result<()> {
    let request = Request::new(LoginUserRequest { username: username.clone(), password });
    let response = metadata_client.login_user(request).await?;
    let LoginUserResponse { status, message, token:_ } = response.into_inner();
    println!("Login response: {}", message);
    if status == proto::common::Status::Ok as i32 {
        println!("Login successful.");
        Ok(())
    } else {
        println!("Login failed with status: {}", status);
        Err(anyhow::anyhow!("Invalid username or password"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let metadata_client = connect_to_metadata_server().await?;
    match args.command {
        Command::Put {
            file_name,
            file_path,
            username,
        } => {
            put_file_chunk(metadata_client, file_name, file_path, username).await?;
        }
        Command::Delete {
            file_name,
            username ,
        } => {
            delete_file(metadata_client, file_name, username).await?;
        }
        Command::Get {
            file_name,
        } => {
            get_file(metadata_client, file_name).await?;
        }
        Command::Register { username, password } => {
            register_user(metadata_client, username, password).await?;
        }
        Command::Login { username, password } => {
            if let Err(_) = login_user(metadata_client.clone(), username.clone(), password).await {
                return Ok(());
            }

            // Prompt for user input
            loop {
                print!("Enter command (put filename filepath | get filename | delete filename | exit): \n");
                io::stdout().flush().unwrap();

                let mut input = String::new();
                io::stdin().read_line(&mut input).unwrap();
                let input = input.trim();

                if input == "exit" {
                    break;
                }

                let parts: Vec<&str> = input.split_whitespace().collect();
                if parts.len() < 2 {
                    println!("Invalid command. Please try again.");
                    continue;
                }

                match parts[0] {
                    "put" => {
                        if parts.len() != 3 {
                            println!("Invalid command. Usage: put filename filepath");
                            continue;
                        }
                        let file_name = parts[1].to_string();
                        let file_path = PathBuf::from(parts[2]);
                        put_file_chunk(metadata_client.clone(), file_name, file_path, username.clone()).await?;
                    }
                    "get" => {
                        if parts.len() != 2 {
                            println!("Invalid command. Usage: get filename");
                            continue;
                        }
                        let file_name = parts[1].to_string();
                        get_file(metadata_client.clone(), file_name).await?;
                    }
                    "delete" => {
                        if parts.len() != 2 {
                            println!("Invalid command. Usage: delete filename");
                            continue;
                        }
                        let file_name = parts[1].to_string();
                        delete_file(metadata_client.clone(), file_name, username.clone()).await?;
                    }
                    _ => {
                        println!("Unknown command. Please try again.");
                    }
                }
            }
        }
    }

    Ok(())
}