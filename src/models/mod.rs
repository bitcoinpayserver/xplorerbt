use bitcoin::Txid;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::str::FromStr;
use tokio_postgres::Row;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PaymentStatus {
    Pending { tx_id: String, amount: Decimal },
    Settled { block_hash: String, confirmations: u32 },
}

#[derive(Deserialize)]
pub struct AddressInfo {
    pub address: String,
    pub webhook_url: String,
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub struct Utxo {
    pub tx_id: String,
    pub vout: i32,
    pub amount: Decimal,
    pub address: Option<String>,
    pub webhook_url: String,
    pub block_height: Option<i64>,
    pub confirmations: Option<i32>,
}

impl Eq for Utxo {}

impl PartialEq for Utxo {
    fn eq(&self, other: &Self) -> bool {
        self.tx_id == other.tx_id &&
            self.vout == other.vout &&
            self.amount == other.amount &&
            self.address == other.address &&
            self.webhook_url == other.webhook_url
    }
}

impl Hash for Utxo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tx_id.hash(state);
        self.vout.hash(state);
        self.amount.hash(state);
        self.address.hash(state);
        self.webhook_url.hash(state);
    }
}

impl Utxo {
    pub fn from_row(row: &Row) -> Self {
        let address: Option<String> = row.get("address");
        let tx_id: Txid = Txid::from_str(row.get("tx_id")).unwrap();
        let vout: i32 = row.get("vout");
        let amount: Decimal = row.get("amount");
        let webhook_url: String = row.get("webhook_url");
        let block_height: Option<i64> = row.get("block_height");
        let confirmations: Option<i32> = row.get("confirmations");
        Self {
            tx_id: tx_id.to_string(),
            vout,
            amount,
            address,
            webhook_url,
            block_height,
            confirmations,
        }
    }

    pub fn update_confirmations(&mut self, confirmations: i32) {
        self.confirmations = Some(confirmations);
    }
}