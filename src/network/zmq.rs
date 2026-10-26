use crate::models::ZMQMessage;
use crate::services::mempool::MempoolService;
use crate::services::tx::TransactionService;
use bitcoin::consensus::deserialize;
use bitcoin::{Block, Transaction};
use std::error::Error;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::spawn_blocking;
use zmq::{Message, SUB, Socket};

pub struct ZMQEngine;

impl ZMQEngine {
    pub async fn start(
        tx_endpoint: &str,
        blk_endpoint: &str,
        mempool_service: Arc<MempoolService>,
        tx_service: Arc<TransactionService>
    ) -> Result<(), Box<dyn Error>> {

        let (tx_sender, mut tx_receiver) = channel::<ZMQMessage>(12000);
        let (block_sender, mut block_receiver) = channel::<ZMQMessage>(12000);

        let tx_endpoint = tx_endpoint.to_string();
        spawn_blocking(move || {
            let tx_socket = Self::subscribe_to_txs(&tx_endpoint).expect("failed to subscribe to txs");
            loop {
                let message = match Self::get_message(&tx_socket) {
                    Ok(message) => message,
                    Err(_) => {
                        // Skip malformed message
                        continue;
                    }
                };
                if let Err(e) = tx_sender.try_send(message) {
                    match e {
                        TrySendError::Full(_) => {
                            sleep(Duration::from_millis(1000));
                        }
                        TrySendError::Closed(_) => {
                            eprintln!("tx receiver is dropped, shutting down zmq tx thread worker");
                            break;
                        }
                    }
                };
            }
        });

        let blk_endpoint = blk_endpoint.to_string();
        spawn_blocking( move || {
            let block_socket = Self::subscribe_to_blocks(&blk_endpoint).expect("failed to subscribe to blocks");
            loop {
                let message = match Self::get_message(&block_socket) {
                    Ok(message) => message,
                    Err(_) => {
                        // Skip malformed message
                        continue
                    }
                };
                if let Err(e) = block_sender.try_send(message) {
                    match e {
                        TrySendError::Full(_) => {
                            sleep(Duration::from_millis(1000));
                        }
                        TrySendError::Closed(_) => {
                            eprintln!("block receiver is dropped, shutting down zmq block thread worker");
                            break
                        }
                    }
                };
            }
        });

        {
            let tx_service = tx_service.clone();
            let mempool_service = mempool_service.clone();
            spawn(async move {
                while let Some(message) = tx_receiver.recv().await {
                    let tx: Transaction = match deserialize(&message.body) {
                        Ok(tx) => tx,
                        Err(e) => {
                            eprintln!("failed to deserialize transaction: {e}, skipping...");
                            continue;
                        }
                    };

                    mempool_service.save_new_utxos(&tx).await;

                    let updated_addresses = mempool_service.get_addresses();
                    tx_service.update_utxos_unconfirmed(&tx, updated_addresses).await;
                }
            });
        }

        spawn( async move {
            while let Some(message) = block_receiver.recv().await {
                let block: Block = match deserialize(&message.body) {
                    Ok(block) => block,
                    Err(e) => {
                        eprintln!("failed to deserialize block: {e}, skipping...");
                        continue;
                    }
                };

                mempool_service.save_block(&block).await;

                tx_service.update_utxo_confirmations(&block).await;
            }
        });

        Ok(())
    }

    fn get_message(socket: &Socket) -> Result<ZMQMessage, Box<dyn Error>> {
        let mut topic = Message::new();
        let mut payload = Message::new();
        let mut sequence = Message::new();
        socket.recv(&mut topic, 0)?;
        socket.recv(&mut payload, 0)?;
        socket.recv(&mut sequence, 0)?;

        let topic = match String::from_utf8(topic.to_vec()){
            Ok(topic) => topic,
            Err(e) => {
                eprintln!("failed to parse tx topic: {e}, skipping to the next incoming bytes");
                return Err(Box::from(e));

            },
        };

        let sequence = u32::from_le_bytes(sequence.to_vec().try_into().unwrap());

        Ok(ZMQMessage::new(topic, payload.to_vec(), sequence))
    }

    fn subscribe_to_txs(tx_endpoint: &str) -> Result<Socket, Box<dyn Error>> {
        let context = zmq::Context::new();
        let tx_socket = context.socket(SUB)?;
        tx_socket.connect(tx_endpoint)?;
        tx_socket.set_subscribe(b"rawtx")?;
        Ok(tx_socket)
    }

    fn subscribe_to_blocks(blk_endpoint: &str) -> Result<Socket, Box<dyn Error>> {
        let context = zmq::Context::new();
        let blk_socket = context.socket(SUB)?;
        blk_socket.connect(blk_endpoint)?;
        blk_socket.set_subscribe(b"rawblock").expect("Failed to set rawblock subscribe");
        Ok(blk_socket)
    }
}