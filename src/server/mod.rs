mod routing;
pub mod config;

use std::sync::Arc;
use axum::serve;
use tokio::net::TcpListener;
use crate::server::config::Config;
use crate::server::routing::routes;
use crate::services::mempool::MempoolService;

pub async fn start_server(config: Config, mempool: Arc<MempoolService>) {
    let tcp_listener = TcpListener::bind(config.xplorer_addr).await.unwrap();
    let router = routes(config, mempool);
    serve(tcp_listener, router).await.unwrap();
}