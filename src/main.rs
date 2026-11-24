mod config;
mod http;
mod metrics;
mod rate_limit;
mod redis_io;
mod state;
mod ws;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::state::AppState;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn redis_url(host: &str, port: u16, password: &Option<String>) -> String {
    match password {
        Some(p) => format!("redis://:{p}@{host}:{port}"),
        None => format!("redis://{host}:{port}"),
    }
}

/// Mapea el nivel numérico de `tslog` (0=trace .. 6=fatal/silent) al
/// filtro de `tracing` más parecido. No es un requisito de protocolo,
/// solo de operación -- el original tampoco dependía de un valor exacto
/// más allá de controlar verbosidad.
fn log_level_to_filter(level: &str) -> &'static str {
    match level.parse::<u8>().unwrap_or(2) {
        0 => "trace",
        1 => "debug",
        2 => "info",
        3 => "warn",
        4..=5 => "error",
        _ => "off",
    }
}

#[tokio::main]
async fn main() {
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error de configuración: {e}");
            std::process::exit(1);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(log_level_to_filter(&config.log_level)))
        .init();

    info!("arrancando signaling-rust");

    let pub_url = redis_url(&config.redis_host_pub, config.redis_port, &config.redis_password);
    let sub_url = redis_url(&config.redis_host_subs, config.redis_port, &config.redis_password);

    let pub_client = match ::redis::Client::open(pub_url) {
        Ok(c) => c,
        Err(e) => {
            error!("no se pudo construir el cliente redis (publisher): {e}");
            std::process::exit(1);
        }
    };
    let redis_conn = match pub_client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            error!("no se pudo conectar a redis (publisher): {e}");
            std::process::exit(1);
        }
    };

    let sub_client = match ::redis::Client::open(sub_url) {
        Ok(c) => c,
        Err(e) => {
            error!("no se pudo construir el cliente redis (subscriber): {e}");
            std::process::exit(1);
        }
    };
    let subscriber = match crate::redis_io::subscriber::spawn_subscriber(sub_client).await {
        Ok(s) => s,
        Err(e) => {
            error!("no se pudo iniciar el subscriber de redis: {e}");
            std::process::exit(1);
        }
    };

    let metrics = Arc::new(Metrics::new());
    let rate_limit_tick = rate_limit::spawn_global_tick(config.rate_limit_ms);
    let config = Arc::new(config);

    let state = Arc::new(AppState {
        redis_conn,
        subscriber,
        rate_limit_tick,
        metrics: metrics.clone(),
        config: config.clone(),
    });

    // Servidor HTTP: /health, /metrics (sección 5 de la spec).
    let http_router = http::router(metrics.clone());
    let http_addr = format!("0.0.0.0:{}", config.http_port);
    let http_listener = TcpListener::bind(&http_addr)
        .await
        .unwrap_or_else(|e| panic!("no se pudo bindear HTTP en {http_addr}: {e}"));
    info!("HTTP (health/metrics) escuchando en {http_addr}");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, http_router).await {
            error!("servidor HTTP terminó con error: {e}");
        }
    });

    // Servidor WebSocket.
    let ws_addr = format!("0.0.0.0:{}", config.port);
    let ws_listener = TcpListener::bind(&ws_addr)
        .await
        .unwrap_or_else(|e| panic!("no se pudo bindear WS en {ws_addr}: {e}"));
    info!("WebSocket escuchando en {ws_addr}");

    loop {
        match ws_listener.accept().await {
            Ok((stream, _peer_addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    ws::connection::handle_connection(stream, state).await;
                });
            }
            Err(e) => {
                error!("error aceptando conexión TCP: {e}");
            }
        }
    }
}
