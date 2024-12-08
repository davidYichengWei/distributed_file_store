use clap::Parser;
use proto::metadata::metadata_server_client::MetadataServerClient;
use proto::metadata::{AddServerRequest, ServerHeartbeatRequest};
use proto::storage::storage_server_server::StorageServerServer;
use std::time::Duration;
use storage_server::config::ServerConfig;
use storage_server::service::StorageService;
use tokio::time;
use tonic::transport::Server;

async fn register_with_metadata_server(
    config: &ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let metadata_addr = format!(
        "http://{}:{}",
        config.metadata_server_address, config.metadata_server_port
    );

    // Create metadata client with timeout
    let channel = tonic::transport::Channel::from_shared(metadata_addr)?
        .timeout(Duration::from_secs(5))
        .connect()
        .await?;

    let mut client = MetadataServerClient::new(channel);

    // Prepare registration request
    let request = tonic::Request::new(AddServerRequest {
        server_address: config.bind_address.clone(),
        server_port: config.port as u32,
    });

    // Attempt to register
    match client.add_server(request).await {
        Ok(response) => {
            let response = response.into_inner();
            if response.status != proto::common::Status::Ok as i32 {
                return Err("Metadata server rejected registration".into());
            }
            Ok(())
        }
        Err(e) => Err(format!("Failed to register with metadata server: {}", e).into()),
    }
}

async fn start_heartbeat(config: ServerConfig) {
    let metadata_addr = format!(
        "http://{}:{}",
        config.metadata_server_address, config.metadata_server_port
    );

    let mut interval = time::interval(Duration::from_secs(config.heartbeat_interval_secs));

    loop {
        interval.tick().await;

        match MetadataServerClient::connect(metadata_addr.clone()).await {
            Ok(mut client) => {
                let request = tonic::Request::new(ServerHeartbeatRequest {
                    server_address: config.bind_address.clone(),
                    server_port: config.port as u32,
                });

                match client.do_server_heartbeat(request).await {
                    Ok(response) => {
                        if response.into_inner().status != proto::common::Status::Ok as i32 {
                            eprintln!("Metadata server rejected heartbeat");
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to send heartbeat: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to metadata server for heartbeat: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments or config file
    let config =
        ServerConfig::from_file("config/config.toml").unwrap_or_else(|_| ServerConfig::parse());

    let addr = config.addr().parse()?;

    // Try to register with metadata server
    if let Err(e) = register_with_metadata_server(&config).await {
        eprintln!(
            "Failed to register with metadata server: {}. Terminating...",
            e
        );
        std::process::exit(1);
    }

    println!("Successfully registered with metadata server");

    // Start heartbeat in background
    let heartbeat_config = config.clone(); // Clone config for the heartbeat task
    tokio::spawn(async move {
        start_heartbeat(heartbeat_config).await;
    });

    // Initialize service
    let storage_service = StorageService::new(config.port).await?;

    println!("Storage Server listening on {}", addr);

    // Start server
    Server::builder()
        .add_service(StorageServerServer::new(storage_service))
        .serve(addr)
        .await?;

    Ok(())
}
