use clap::Parser;
use proto::metadata::metadata_server_server::MetadataServerServer;
use metadata_server::config::ServerConfig;
use metadata_server::service::MetadataService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let config = ServerConfig::parse();
    let addr = config.addr().parse()?;

    // Initialize service with port number
    let metadata_service = MetadataService::new().await?;

    println!("Metadata Server listening on {}", addr);

    // Start server
    Server::builder()
        .add_service(MetadataServerServer::new(metadata_service))
        .serve(addr)
        .await?;

    Ok(())
}
