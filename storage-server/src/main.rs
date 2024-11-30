use clap::Parser;
use proto::storage::storage_server_server::StorageServerServer;
use storage_server::config::ServerConfig;
use storage_server::service::StorageService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let config = ServerConfig::parse();
    let addr = config.addr().parse()?;

    // Initialize service with port number
    let storage_service = StorageService::new(config.port).await?;

    println!("Storage Server listening on {}", addr);

    // Start server
    Server::builder()
        .add_service(StorageServerServer::new(storage_service))
        .serve(addr)
        .await?;

    Ok(())
}
