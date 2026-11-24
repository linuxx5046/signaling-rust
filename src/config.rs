use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub redis_host_pub: String,
    pub redis_host_subs: String,
    pub redis_port: u16,
    pub redis_password: Option<String>,
    pub log_level: String,
    pub port: u16,
    pub http_port: u16,
    pub rate_limit_messages: u32,
    pub rate_limit_ms: u64,
    /// Punto de partida para el ping/pong de housekeeping (sección 2.5 de la
    /// especificación). En el original este valor existía (`heartbeatInterval`)
    /// pero nunca se usaba. Acá sí se usa.
    pub heartbeat_interval_ms: u64,
}

fn get_env_required(key: &str) -> Result<String, String> {
    env::var(key).map_err(|_| format!("missing env: {key}"))
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Ok(Config {
            redis_host_pub: get_env_required("REDIS_HOST_PUB")?,
            redis_host_subs: get_env_required("REDIS_HOST_SUBS")?,
            redis_port: get_env_required("REDIS_PORT")?
                .parse()
                .map_err(|_| "REDIS_PORT must be a valid port number".to_string())?,
            redis_password: env::var("REDIS_PASSWORD").ok(),
            log_level: get_env_required("LOG_LEVEL")?,
            port: get_env_required("PORT")?
                .parse()
                .map_err(|_| "PORT must be a valid port number".to_string())?,
            http_port: get_env_required("HTTP_PORT")?
                .parse()
                .map_err(|_| "HTTP_PORT must be a valid port number".to_string())?,
            rate_limit_messages: env::var("RATE_LIMIT_MESSAGES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            rate_limit_ms: env::var("RATE_LIMIT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            heartbeat_interval_ms: env::var("HEARTBEAT_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30_000),
        })
    }
}
