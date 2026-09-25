use std::net::SocketAddr;
use bitcoin::Network;

#[derive(Clone)]
pub struct Config {
    pub xplorer_addr: SocketAddr,
    pub tx_url: String,
    pub blocks_url: String,
    pub db_host: String,
    pub db_name: String,
    pub db_username: String,
    pub db_password: String,
    pub network: Network,
}

impl Config {
    pub fn new() -> Self {
        Self {
            xplorer_addr: "0.0.0.0:8000".parse().unwrap(),
            tx_url: "tcp://127.0.0.1:28332".to_string(),
            blocks_url: "tcp://127.0.0.1:28333".to_string(),
            db_host: "localhost".to_string(),
            db_name: "postgres".to_string(),
            db_username: "postgres".to_string(),
            db_password: "".to_string(),
            network: Network::Regtest,
        }
    }
}