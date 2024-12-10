use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[clap(long, default_value = "127.0.0.1")]
    pub bind_address: String,
    #[clap(long, default_value = "50051")]
    pub port: u16,
    #[clap(long, default_value = "127.0.0.1")]
    pub metadata_server_address: String,
    #[clap(long, default_value = "50052")]
    pub metadata_server_port: u16,
    #[clap(long, default_value = "1")]
    pub heartbeat_interval_secs: u64,
}

impl ServerConfig {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }

    pub fn from_file(path: &str) -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .build()?;
        settings.try_deserialize()
    }
}
