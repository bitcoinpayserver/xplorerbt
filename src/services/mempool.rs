use crate::services::block::BlockService;
use crate::services::tx::TransactionService;
use crate::storage::database::Database;
use bitcoin::{Address, Block, Network, Transaction};
use chrono::DateTime;
use dashmap::DashMap;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
pub struct MempoolService {
    addresses: Arc<DashMap<Address, String>>,
    db: Arc<Database>,
    network: Network
}

impl MempoolService  {
    const INSERT_TX_OUT: &'static str = "INSERT INTO xbt_tx_outs (tx_id, vout, amount, address, webhook_url) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tx_id, vout) DO NOTHING RETURNING id";

    const INSERT_BLOCK: &'static str = "INSERT INTO xbt_blocks (hash, prev_hash, height, timestamp) VALUES ($1, $2, $3, $4) RETURNING id";

    pub async fn new(db: Arc<Database>, network: Network) -> MempoolService {
        let addresses = Arc::new(
            Self::load_addresses(db.clone(), network).await
        );
        Self {
            addresses,
            db,
            network
        }
    }

    pub fn add_address(&mut self, address: Address, webhook_url: String) {
        self.addresses.insert(address, webhook_url);
    }

    pub async fn save_new_utxos(&self, tx: &Transaction) {
        let tx_id = tx.compute_txid();
        let mut vout = 0;
        let utxos: Vec<(i32, Decimal, Option<Address>)> = tx.output.iter().map(|output| {
            let address = Address::from_script(output.script_pubkey.as_script(), self.network);
            let address = match  address {
                Ok(address) => Some(address),
                Err(_) => None
            };
            let amount = Decimal::new(output.value.to_sat() as i64, 0);
            let vout_amount_address = (vout, amount, address);
            vout = vout + 1;
            vout_amount_address
        }
        ).collect();

        let mut conn = self.db.pool.get().await.unwrap();
        let db_tx = conn.transaction().await.unwrap();

        for (vout, amount, address) in utxos {
            if address.is_none() { continue }
            let address = address.unwrap();
            if !self.addresses.contains_key(&address) { continue }
            let webhook_url = self.addresses.get(&address).unwrap();
            match Database::tx_query_opt(&db_tx, Self::INSERT_TX_OUT, &[&tx_id.to_string(), &vout, &amount, &address.to_string(), webhook_url.value()]).await {
                Ok(_) => (),
                Err(e) => {
                     eprintln!("failed to insert tx output {}:{}\n{:?}", tx_id, vout, e);
                }
            };
        }
        db_tx.commit().await.unwrap();
    }

    pub async fn save_block(&self, block: &Block) {
        let block_hash = block.block_hash().to_string();
        let prev_hash = block.header.prev_blockhash.to_string();

        let height = BlockService::block_height(&block);

        let time = block.header.time as i64;
        let timestamp = DateTime::from_timestamp_secs(time);

        let mut conn = self.db.pool.get().await.unwrap();
        let db_tx = conn.transaction().await.unwrap();

        Database::tx_query_one(&db_tx, Self::INSERT_BLOCK, &[&block_hash, &prev_hash, &height, &timestamp]).await.unwrap();

        db_tx.commit().await.unwrap();
    }

    pub fn get_addresses(&self) -> Arc<DashMap<Address, String>> {
        self.addresses.clone()
    }

    async fn load_addresses(db: Arc<Database>, network: Network) -> DashMap<Address, String> {
        let addresses: DashMap<Address, String> = DashMap::new();
        let utxos = TransactionService::load_utxos(db).await;
        for utxo in utxos.iter() {
            let address = utxo.address.clone().unwrap();
            let address = Address::from_str(address.as_str()).unwrap()
                .require_network(network).unwrap();

            addresses.insert(address, utxo.webhook_url.clone());
        }
        addresses
    }
}