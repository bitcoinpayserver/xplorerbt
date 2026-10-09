use crate::services::mempool::MempoolService;
use bitcoin::consensus::deserialize;
use bitcoin::{Block, Transaction};
use std::error::Error;
use std::sync::Arc;
use tokio::spawn;
use tokio::task::spawn_blocking;
use zmq::{Socket, SUB};
use crate::services::tx::TransactionService;

pub struct ZMQEngine;

impl ZMQEngine {
    pub async fn start(
        tx_endpoint: &str,
        blk_endpoint: &str,
        mempool: Arc<MempoolService>,
        tx_service: Arc<TransactionService>
    ) -> Result<(), Box<dyn Error>> {
        let tx_socket = Self::subscribe_to_txs(tx_endpoint).await?;
        let block_socket = Self::subscribe_to_blocks(blk_endpoint).await?;

        let tx_mempool = mempool.clone();
        let block_mempool = mempool;

        let tx_service = tx_service.clone();
        let block_tx_service = tx_service.clone();

        spawn_blocking(move || {
            let tx_socket = tx_socket;
            loop {
                let _ = match tx_socket.recv_bytes(0) {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("failed to get first bytes: {}", e);
                        vec![]
                    }
                };
                let payload = match tx_socket.recv_bytes(0) {
                    Ok(payload) => payload,
                    Err(e) => {
                        eprintln!("failed to get payload: {}", e);
                        vec![]
                    }
                };

                if tx_socket.get_rcvmore().unwrap_or(false) {
                    let _ = match tx_socket.recv_bytes(0) {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("failed to get last bytes: {}", e);
                            vec![]
                        }
                    };
                }

                let tx_mempool = tx_mempool.clone();
                let tx_service = tx_service.clone();
                spawn(async move {
                    let tx: Transaction = deserialize(&payload).unwrap_or_else(|e| {
                        eprintln!("failed to deserialize transaction: {}", e);
                        panic!()
                    });

                    tx_mempool.save_new_utxos(&tx).await;

                    let updated_addresses = tx_mempool.get_addresses();
                    tx_service.update_utxos_unconfirmed(&tx, updated_addresses).await;
                });
            }});

        spawn_blocking( move || {
            loop {
                let _ = match block_socket.recv_bytes(0) {
                    Ok(data) => data,
                    Err(_) => break
                };
                let payload = match block_socket.recv_bytes(0) {
                    Ok(payload) => payload,
                    Err(_) => break
                };
                if block_socket.get_rcvmore().unwrap_or(false) {
                    let _ = match block_socket.recv_bytes(0) {
                        Ok(data) => data,
                        Err(_) => break
                    };
                }

                let block_mempool = block_mempool.clone();
                let block_tx_service = block_tx_service.clone();
                spawn(async move {
                    let block: Block = deserialize(&payload).unwrap_or_else(|e| {
                        eprintln!("{:?}", e);
                        panic!()
                    });

                    block_mempool.save_block(&block).await;

                    block_tx_service.update_utxo_confirmations(&block).await;
                });
            }
        });

        Ok(())
    }
    async fn subscribe_to_txs(tx_endpoint: &str) -> Result<Socket, Box<dyn Error>> {
        let context = zmq::Context::new();
        let tx_socket = context.socket(SUB)?;
        tx_socket.connect(tx_endpoint)?;
        tx_socket.set_subscribe(b"rawtx")?;
        Ok(tx_socket)
    }

    async fn subscribe_to_blocks(blk_endpoint: &str) -> Result<Socket, Box<dyn Error>> {
        let context = zmq::Context::new();
        let blk_socket = context.socket(SUB)?;
        blk_socket.connect(blk_endpoint)?;
        blk_socket.set_subscribe(b"rawblock").expect("Failed to set rawblock subscribe");
        Ok(blk_socket)
    }
}