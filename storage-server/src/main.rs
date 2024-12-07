use clap::Parser;
use proto::metadata::metadata_server_client::MetadataServerClient;
use proto::metadata::AddServerRequest;
use proto::storage::storage_server_server::StorageServerServer;
use std::time::Duration;
use storage_server::config::ServerConfig;
use storage_server::service::StorageService;
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

    // Initialize service
    let storage_service = StorageService::new(config.port).await?;

    println!("Successfully registered with metadata server");
    println!("Storage Server listening on {}", addr);

    // Start server
    Server::builder()
        .add_service(StorageServerServer::new(storage_service))
        .serve(addr)
        .await?;

    Ok(())
}
