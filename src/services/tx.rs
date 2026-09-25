use crate::models::Utxo;
use crate::services::block::BlockService;
use crate::storage::database::Database;
use bitcoin::{Address, Block, Network, Transaction};
use dashmap::DashMap;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct TransactionService {
    utxos: Arc<DashMap<String, Utxo>>,
    db: Arc<Database>,
    utxo_sender: Arc<UnboundedSender<Utxo>>,
    network: Network
}

impl TransactionService {
    pub const CREATE_TABLE: &'static str = "CREATE TABLE IF NOT EXISTS xbt_tx_outs (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        tx_id VARCHAR(64) NOT NULL,
        vout INT NOT NULL,
        amount NUMERIC(24) NOT NULL,
        address VARCHAR(164)  DEFAULT NULL,
        block_height BIGINT DEFAULT NULL,
        confirmations INT DEFAULT NULL,
        webhook_url VARCHAR(164)  DEFAULT NULL,
        detected_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(tx_id, vout)
    )";

    pub(crate) const GET_ALL: &'static str = "SELECT * FROM xbt_tx_outs;";

    const UPDATE_TX_OUT_CONFIRMATIONS: &'static str = "UPDATE xbt_tx_outs SET confirmations = $1 WHERE tx_id = $2 AND vout = $3 RETURNING (tx_id, vout);";

    const UPDATE_TX_OUT_BLOCK_HEIGHT: &'static str = "UPDATE xbt_tx_outs SET block_height = $1 WHERE tx_id = $2 AND vout = $3 RETURNING (tx_id, vout);";

    pub async fn new(db: Arc<Database>, utxo_sender: Arc<UnboundedSender<Utxo>>, network: Network) -> Self {
        let utxos = Arc::new(Self::load_utxos(db.clone()).await);
        Self {
            utxos,
            db,
            utxo_sender,
            network
        }
    }

    pub async fn update_utxos_unconfirmed(&self, tx: &Transaction, addresses: Arc<DashMap<Address, String>>) {
        let tx_id = tx.compute_txid();
        let mut vout = 0;

        for output in &tx.output {
            let address = match Address::from_script(output.script_pubkey.as_script(), self.network) {
                Ok(address) => Some(address),
                Err(_) => None
            };

            let address_string = address.clone().map(|address| address.to_string());

            if address.is_some() {
                let address = address.unwrap();
                if !addresses.contains_key(&address) {
                    vout = vout + 1;
                    continue;
                }

                let webhook_url = addresses.get(&address).unwrap().value().clone();

                let amount = Decimal::new(output.value.to_sat() as i64, 0);
                let utxo = Utxo {
                    tx_id: tx_id.to_string(),
                    vout,
                    amount,
                    address: address_string,
                    webhook_url,
                    block_height: None,
                    confirmations: None,
                };

                self.utxo_sender.send(utxo.clone()).unwrap();

                self.add_utxo(utxo);
            }

            vout = vout + 1;
        }
    }

    pub async fn update_utxo_confirmations(&self, block: &Block) {
        let mut conn = self.db.pool.get().await.unwrap();
        let db_tx = conn.transaction().await.unwrap();

        let outpoints: Vec<String> = self.utxos.iter().map(|utxo| utxo.key().clone()).collect();
        let height = BlockService::block_height(&block);

        for tx in &block.txdata {
            let tx_id = tx.compute_txid().to_string();
            for outpoint in &outpoints {
                let mut utxo = self.utxos.get_mut(outpoint).unwrap();
                if utxo.confirmations.is_none() && utxo.tx_id == tx_id {
                    utxo.update_confirmations(1);

                    utxo.block_height = Some(height as i64);

                    self.utxo_sender.send(utxo.value().clone()).unwrap();

                    match Database::tx_query_one(&db_tx, Self::UPDATE_TX_OUT_CONFIRMATIONS, &[&1, &tx_id, &utxo.vout]).await {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("failed to update tx out confirmations {:?}", e);
                        }
                    };
                    match Database::tx_query_one(&db_tx, Self::UPDATE_TX_OUT_BLOCK_HEIGHT, &[&(height as i64), &tx_id, &utxo.vout]).await {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("failed to update tx out block height {:?}", e);
                        }
                    };
                }
            }
        }

        for outpoint in &outpoints {
            let mut utxo = self.utxos.get_mut(outpoint).unwrap();
            let mut confirmations = utxo.confirmations.unwrap_or(0);

            if confirmations > 0 {
                confirmations = 1 + ((height as i64) - utxo.block_height.unwrap()) as i32;
                utxo.update_confirmations(confirmations);

                match self.utxo_sender.send(utxo.value().clone()) {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("failed to send utxo to broadcaster {:?}", e.to_string());
                    }
                };
                
                match Database::tx_query_one(&db_tx, Self::UPDATE_TX_OUT_CONFIRMATIONS, &[&confirmations, &utxo.tx_id, &utxo.vout]).await {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("failed to update tx out confirmations {:?}", e);
                    }
                };
            }
        }

        db_tx.commit().await.unwrap();
    }

    pub async fn start_broadcasting_txs(mut utxo_receiver: UnboundedReceiver<Utxo>) {
        let client = reqwest::Client::new();

        spawn(async move {
            while let Some(utxo) = utxo_receiver.recv().await {
                match client.post(&utxo.webhook_url)
                    .json(&utxo)
                    .send().await {
                    Ok(response) => response,
                    Err(e) => {
                        eprintln!("failed to broadcast output: {}:{}", utxo.tx_id, utxo.vout);
                        eprintln!("{}", e);
                        continue;
                    }
                };
            }
        });
    }

    pub async fn load_utxos(db: Arc<Database>) -> DashMap<String, Utxo> {
        let tx_outs_rows = db.query_all(
            TransactionService::GET_ALL,
            &[]
        ).await;

        let utxos: DashMap<String, Utxo> = DashMap::new();
        for row in tx_outs_rows {
            let utxo = Utxo::from_row(&row);
            let outpoint = format!("{}:{}", utxo.tx_id, utxo.vout);
            utxos.insert(outpoint, utxo);
        }
        utxos
    }

    fn add_utxo(&self, utxo: Utxo) {
        let outpoint = format!("{}:{}", utxo.tx_id, utxo.vout);
        self.utxos.insert(outpoint, utxo);
    }
}