use std::sync::Arc;
use bitcoin::Block;
use crate::storage::database::Database;

pub struct BlockService {
    db: Arc<Database>
}

impl BlockService {
    pub const CREATE_TABLE: &'static str = "CREATE TABLE IF NOT EXISTS xbt_blocks (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        hash VARCHAR(64) NOT NULL UNIQUE,
        prev_hash VARCHAR(64) NOT NULL UNIQUE,
        height INT NOT NULL UNIQUE,
        timestamp TIMESTAMPTZ NOT NULL
    )";

    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn block_height(block: &Block) -> i32 {
        if block.header.v2.is_some() {
            block.header.v2.unwrap().height
        } else {
            block.bip34_block_height().unwrap() as i32
        }
    }
}