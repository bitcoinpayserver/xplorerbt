mod network;
mod services;
mod storage;
mod server;
mod models;

use crate::network::zmq::ZMQEngine;
use crate::server::start_server;
use crate::services::block::BlockService;
use crate::services::mempool::MempoolService;
use crate::services::tx::TransactionService;
use crate::storage::database::Database;
use server::config::Config;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::new();

    let db = Arc::new(Database::new(&config));
    db.create_tables(&[TransactionService::CREATE_TABLE, BlockService::CREATE_TABLE]).await;

    let mempool = Arc::new(MempoolService::new(db.clone(), config.network).await);
    let tx_service = Arc::new(
        TransactionService::new(db, config.network).await
    );

    ZMQEngine::start(
        config.tx_url.as_str(),
        config.blocks_url.as_str(),
        mempool.clone(),
        tx_service.clone(),
    ).await?;
    
    start_server(config, mempool).await;

    Ok(())
}
