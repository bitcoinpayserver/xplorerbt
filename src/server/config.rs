use std::env::var;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
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
    const HOST: &'static str = "HOST";
    const PORT: &'static str = "PORT";
    const ZMQ_TX_URL: &'static str = "ZMQ_TX_URL";
    const ZMQ_BLOCKS_URL: &'static str = "ZMQ_BLOCKS_URL";
    const DB_HOST: &'static str = "DB_HOST";
    const DB_NAME: &'static str = "DB_NAME";
    const DB_USER: &'static str = "DB_USER";
    const DB_PASSWORD: &'static str = "DB_PASSWORD";
    const NETWORK: &'static str = "NETWORK";

    pub fn new() -> Self {
        let host: IpAddr = var(Self::HOST).unwrap_or_else(|_| "0.0.0.0".to_string()).parse().unwrap();
        let port: u16 = var(Self::PORT).unwrap_or_else(|_| "8000".to_string()).parse().unwrap();
        let xplorer_addr = SocketAddr::new(host, port);
        let zmq_tx_url = var(Self::ZMQ_TX_URL).unwrap_or_else(|_| "tcp://127.0.0.1:28332".to_string());
        let zmq_block_url = var(Self::ZMQ_BLOCKS_URL).unwrap_or_else(|_| "tcp://127.0.0.1:28333".to_string());
        let db_host = var(Self::DB_HOST).unwrap_or_else(|_| "localhost".to_string());
        let db_name = var(Self::DB_NAME).unwrap_or_else(|_| "postgres".to_string());
        let db_username = var(Self::DB_USER).unwrap_or_else(|_| "postgres".to_string());
        let db_password = var(Self::DB_PASSWORD).unwrap_or_else(|_| "".to_string());
        let network = var(Self::NETWORK).unwrap_or_else(|_| "regtest".to_string());

        Self {
            xplorer_addr,
            tx_url: zmq_tx_url,
            blocks_url: zmq_block_url,
            db_host,
            db_name,
            db_username,
            db_password,
            network: Network::from_str(network.as_str()).unwrap(),
        }
    }
}