// src/main.rs
use clap::Parser;
use proto::metadata::metadata_server_server::MetadataServerServer;
use test_metadata_server::config::ServerConfig;
use test_metadata_server::service::MetadataService;
use tonic::transport::Server;

mod config;
mod service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let config = ServerConfig::parse();
    let addr = config.addr().parse()?;

    // Initialize service
    let metadata_service = MetadataService::new().await?;

    println!("Metadata Server listening on {}", addr);

    // Start server
    Server::builder()
        .add_service(MetadataServerServer::new(metadata_service))
        .serve(addr)
        .await?;

    Ok(())
}