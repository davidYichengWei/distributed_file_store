use clap::Parser;
#[derive(Parser, Debug)]
pub struct ServerConfig {
    #[clap(long, default_value = "127.0.0.1:50052")]
    pub addr: String,
}

impl ServerConfig {
    pub fn addr(&self) -> &str {
        &self.addr
    }
}