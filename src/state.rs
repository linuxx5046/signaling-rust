use crate::config::Config;
use crate::metrics::Metrics;
use crate::redis_io::subscriber::SubscriberHandle;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub struct AppState {
    pub redis_conn: ::redis::aio::MultiplexedConnection,
    pub subscriber: SubscriberHandle,
    pub rate_limit_tick: Arc<AtomicU64>,
    pub metrics: Arc<Metrics>,
    pub config: Arc<Config>,
}
