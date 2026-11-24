//! Contrato HTTP (sección 5 de la especificación): /health y /metrics,
//! sin importar el método, 404 para cualquier otra ruta.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use std::sync::Arc;

use crate::metrics::Metrics;

async fn health_handler() -> &'static str {
    "ok"
}

async fn metrics_handler(
    axum::extract::State(metrics): axum::extract::State<Arc<Metrics>>,
) -> String {
    metrics.render()
}

async fn fallback_handler(_req: Request<Body>) -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

pub fn router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/health", any(health_handler))
        .route("/metrics", any(metrics_handler))
        .fallback(any(fallback_handler))
        .with_state(metrics)
}
