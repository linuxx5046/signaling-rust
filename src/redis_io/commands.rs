//! Comandos de registro/consulta sobre la conexión multiplexada
//! (sección 2.3 de `arquitectura-rust-v1.md`). Timings replican los
//! Gauges del original (sección 4.2 / 6 de la especificación).

use crate::metrics::Metrics;
use redis::AsyncCommands;
use std::time::Instant;

const REGISTRY_TTL_SECONDS: i64 = 43_200; // 12h, igual que el original

/// `SADD key value` + `EXPIRE key 43200`. Nota: el EXPIRE se renueva en
/// cada SADD, igual que el original (sección 4.1 de la spec) -- cada
/// reconexión del mismo rol extiende el TTL de todo el Set.
pub async fn register(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    value: &str,
    metrics: &Metrics,
) -> redis::RedisResult<()> {
    let t0 = Instant::now();
    let _: () = conn.sadd(key, value).await?;
    let _: () = conn.expire(key, REGISTRY_TTL_SECONDS).await?;
    metrics.redis_set_time.set(t0.elapsed().as_secs_f64() * 1000.0);
    Ok(())
}

/// `SREM key value`.
pub async fn deregister(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    value: &str,
    metrics: &Metrics,
) -> redis::RedisResult<()> {
    let t0 = Instant::now();
    let _: () = conn.srem(key, value).await?;
    metrics.redis_delete_time.set(t0.elapsed().as_secs_f64() * 1000.0);
    Ok(())
}

/// `SISMEMBER key value`. El original no mide timing para esta operación
/// (ver auditoría: `isMember` llama a `sIsMember` directo, sin t0/t1), así
/// que acá tampoco se agrega un Gauge nuevo para no inventar una métrica
/// que el original no tenía.
pub async fn is_member(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    value: &str,
) -> redis::RedisResult<bool> {
    conn.sismember(key, value).await
}

/// `SMEMBERS key`.
pub async fn get_target_ids(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    metrics: &Metrics,
) -> redis::RedisResult<Vec<String>> {
    let t0 = Instant::now();
    let ids: Vec<String> = conn.smembers(key).await?;
    metrics.redis_get_time.set(t0.elapsed().as_secs_f64() * 1000.0);
    Ok(ids)
}
