mod network;
mod services;
mod storage;
mod server;
mod models;

use std::sync::Arc;
use server::config::Config;
use crate::models::Utxo;
use crate::network::zmq::ZMQEngine;
use crate::server::start_server;
use crate::services::block::BlockService;
use crate::services::mempool::MempoolService;
use crate::services::tx::TransactionService;
use crate::storage::database::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::new();
    let db = Arc::new(Database::new(&config));
    db.create_tables(&[TransactionService::CREATE_TABLE, BlockService::CREATE_TABLE]).await;

    let (utxo_sender, utxo_receiver) = tokio::sync::mpsc::unbounded_channel::<Utxo>();

    let mempool = Arc::new(MempoolService::new(db.clone(), config.network).await);
    let tx_service = Arc::new(TransactionService::new(db, Arc::new(utxo_sender), config.network).await);

    ZMQEngine::start(
        config.tx_url.as_str(),
        config.blocks_url.as_str(),
        mempool.clone(),
        tx_service.clone(),
    ).await?;

    TransactionService::start_broadcasting_txs(utxo_receiver).await;

    start_server(config, mempool).await;

    Ok(())
}
