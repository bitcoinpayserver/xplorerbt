use crate::models::AddressInfo;
use crate::server::config::Config;
use crate::services::mempool::MempoolService;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    config: Config,
    mempool: Arc<MempoolService>,
}

pub fn routes(
    config: Config,
    mempool: Arc<MempoolService>
) -> Router {
    let state = AppState { config, mempool };

    Router::new()
        .route("/", get(heartbeat))
        .route("/api/v1/check/payment", post(check_payment))
        .with_state(state)
}

async fn heartbeat() -> impl IntoResponse {
    (StatusCode::OK, "XplorerBT")
}

async fn check_payment(
    State(mut state): State<AppState>,
    Json(info): Json<AddressInfo>,
) -> impl IntoResponse {
    let address = bitcoin::Address::from_str(info.address.as_str()).unwrap()
        .require_network(state.config.network).unwrap();
    let webhook_url = info.webhook_url;

    let mempool = Arc::make_mut(&mut state.mempool);
    mempool.add_address(address, webhook_url);
}