use crate::models::Utxo;
use crate::services::block::BlockService;
use crate::storage::database::Database;
use bitcoin::{Address, Block, Network, Transaction};
use dashmap::DashMap;
use reqwest::Client;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::spawn;
use url::Url;

#[derive(Clone)]
pub struct TransactionService {
    utxos: Arc<DashMap<String, Utxo>>,
    http_client: Client,
    db: Arc<Database>,
    network: Network
}

impl TransactionService {
    pub const CREATE_TABLE: &'static str = "CREATE TABLE IF NOT EXISTS xbt_tx_outs (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        tx_id VARCHAR(64) NOT NULL,
        v_out INT NOT NULL,
        amount NUMERIC(24) NOT NULL,
        address VARCHAR(164)  DEFAULT NULL,
        block_height BIGINT DEFAULT NULL,
        confirmations INT DEFAULT NULL,
        webhook_url VARCHAR(164)  DEFAULT NULL,
        detected_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(tx_id, v_out)
    )";

    pub(crate) const GET_ALL: &'static str = "SELECT * FROM xbt_tx_outs;";

    const UPDATE_TX_OUT_CONFIRMATIONS: &'static str = "UPDATE xbt_tx_outs SET confirmations = $1 WHERE tx_id = $2 AND v_out = $3 RETURNING (tx_id, v_out);";

    const UPDATE_TX_OUT_BLOCK_HEIGHT: &'static str = "UPDATE xbt_tx_outs SET block_height = $1 WHERE tx_id = $2 AND v_out = $3 RETURNING (tx_id, v_out);";

    pub async fn new(db: Arc<Database>, network: Network) -> Self {
        let utxos = Arc::new(Self::load_utxos(db.clone()).await);
        let client = Client::new();
        Self {
            utxos,
            http_client: client,
            db,
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
                    v_out: vout,
                    amount,
                    address: address_string,
                    webhook_url,
                    block_height: None,
                    confirmations: Some(0),
                };

                self.add_utxo(utxo.clone());

                self.broadcast_tx_out(utxo).await;
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

                if utxo.confirmations == Some(0) && utxo.tx_id == tx_id {
                    utxo.update_confirmations(1);

                    utxo.block_height = Some(height as i64);

                    self.broadcast_tx_out(utxo.value().clone()).await;

                    match Database::tx_query_one(&db_tx, Self::UPDATE_TX_OUT_CONFIRMATIONS, &[&1, &tx_id, &utxo.v_out]).await {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("failed to update tx out confirmations {:?}", e);
                        }
                    };
                    match Database::tx_query_one(&db_tx, Self::UPDATE_TX_OUT_BLOCK_HEIGHT, &[&(height as i64), &tx_id, &utxo.v_out]).await {
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

                self.broadcast_tx_out(utxo.value().clone()).await;

                match Database::tx_query_one(&db_tx, Self::UPDATE_TX_OUT_CONFIRMATIONS, &[&confirmations, &utxo.tx_id, &utxo.v_out]).await {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("failed to update tx out confirmations {:?}", e);
                    }
                };
            }
        }

        db_tx.commit().await.unwrap();
    }

    async fn broadcast_tx_out(&self, utxo: Utxo) {
        let client = self.http_client.clone();
        spawn(async move {
            match client.post(&utxo.webhook_url)
                .json(&utxo)
                .send().await {
                Ok(_) => {
                    let url = Url::parse(&utxo.webhook_url).unwrap();
                    let (_, tracking_id_value) = url.query_pairs().next().unwrap();
                    let tracking_id = tracking_id_value.to_string();
                    println!("\ntx output \"{}:{}\" sent to payment \"{tracking_id}\"", utxo.tx_id, utxo.v_out);
                },
                Err(_) => {
                    eprintln!("failed to broadcast tx output \"{}:{}\"", utxo.tx_id, utxo.v_out);
                }
            };
        });
    }

    fn add_utxo(&self, utxo: Utxo) {
        let outpoint = format!("{}:{}", utxo.tx_id, utxo.v_out);
        self.utxos.insert(outpoint, utxo);
    }

    pub async fn load_utxos(db: Arc<Database>) -> DashMap<String, Utxo> {
        let tx_outs_rows = db.query_all(
            TransactionService::GET_ALL,
            &[]
        ).await;

        let utxos: DashMap<String, Utxo> = DashMap::new();
        for row in tx_outs_rows {
            let utxo = Utxo::from_row(&row);
            let outpoint = format!("{}:{}", utxo.tx_id, utxo.v_out);
            utxos.insert(outpoint, utxo);
        }
        utxos
    }
}