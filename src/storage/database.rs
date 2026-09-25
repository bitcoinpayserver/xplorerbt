use std::str::FromStr;
use deadpool_postgres::{Manager, Pool, Transaction};
use tokio_postgres::{Error, NoTls, Row};
use tokio_postgres::types::ToSql;
use crate::server::config::Config;

pub struct Database {
    pub pool: Pool
}

impl Database {
    const MAX_CONNECTIONS: usize = 10;

    pub fn new(config: &Config) -> Self {
        let connection_string = format!(
                "host={} user={} password={} dbname={}",
                config.db_host,
                config.db_username,
                config.db_password,
                config.db_name
            );

        let postgres_config = tokio_postgres::Config::from_str(connection_string.as_str()).unwrap();

        let pool_manager = Manager::new(postgres_config, NoTls);

        let pool = Pool::builder(pool_manager)
            .max_size(Self::MAX_CONNECTIONS).build().unwrap();

        Self { pool }
    }

    pub async fn create_tables(&self, commands: &[&str]) {
        let mut client = self.pool.get().await.unwrap();
        let transaction = client.transaction().await.unwrap();
        for command in commands {
            transaction.batch_execute(command).await.unwrap();
        }
        transaction.commit().await.unwrap();
    }

    pub async fn query_one(&self, command: &str, data: &[&(dyn Sync + ToSql)]) -> Row {
        let client = self.pool.get().await.unwrap();
        let statement = client.prepare(command).await.unwrap();
        let row = client.query_one(&statement, data).await.unwrap();
        row
    }

    pub async fn query_all(&self, command: &str, data: &[&(dyn Sync + ToSql)]) -> Vec<Row> {
        let client = self.pool.get().await.unwrap();
        let statement = client.prepare(command).await.unwrap();
        let rows = client.query(&statement, data).await.unwrap();
        rows
    }

    pub async fn tx_query_one(tx: &Transaction<'_>, query: &str, args: &[&(dyn ToSql + Sync)]) -> Result<Row, Error> {
        let statement = tx.prepare(query).await?;
        let row = tx.query_one(&statement, args).await?;
        Ok(row)
    }

    pub async fn tx_query_opt(tx: &Transaction<'_>, query: &str, args: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>, Error> {
        let statement = tx.prepare(query).await?;
        let row = tx.query_opt(&statement, args).await?;
        Ok(row)
    }
}