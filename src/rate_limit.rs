//! Replica exacta del algoritmo de ventana fija del original
//! (sección 7 de la especificación de compatibilidad).
//!
//! Comportamiento no obvio que se preserva deliberadamente:
//! - el primer mensaje de cada ventana nunca se rechaza
//! - el límite efectivo real es `limit + 1` antes del primer rechazo
//! - las ventanas de distintas conexiones no están alineadas entre sí

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Tick global compartido por todo el proceso. Lo incrementa una única
/// tarea con `tokio::time::interval`, igual que el `setInterval` único
/// del original.
pub fn spawn_global_tick(interval_ms: u64) -> Arc<AtomicU64> {
    let tick = Arc::new(AtomicU64::new(0));
    let tick_clone = tick.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            tick_clone.fetch_add(1, Ordering::Relaxed);
        }
    });
    tick
}

/// Estado del rate limiter, local a cada conexión (no se comparte entre
/// tareas, así que no necesita sincronización propia más allá de leer el
/// tick global).
pub struct ConnectionRateLimiter {
    last_tick_seen: Option<u64>,
    count: u32,
    limit: u32,
}

impl ConnectionRateLimiter {
    pub fn new(limit: u32) -> Self {
        Self {
            last_tick_seen: None,
            count: 0,
            limit,
        }
    }

    /// Devuelve `true` si este mensaje debe RECHAZARSE (rate limit hit).
    pub fn check(&mut self, global_tick: &Arc<AtomicU64>) -> bool {
        let now = global_tick.load(Ordering::Relaxed);
        if self.last_tick_seen != Some(now) {
            self.last_tick_seen = Some(now);
            self.count = 1;
            false // el primer mensaje de cada ventana nunca se rechaza
        } else {
            self.count += 1;
            self.count > self.limit
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_message_of_window_never_rejected_even_at_limit() {
        let tick = Arc::new(AtomicU64::new(0));
        let mut limiter = ConnectionRateLimiter::new(2);
        // dentro de la misma ventana: msg1 ok, msg2 ok (count=2, no excede),
        // msg3 excede (count=3 > 2)
        assert_eq!(limiter.check(&tick), false);
        assert_eq!(limiter.check(&tick), false);
        assert_eq!(limiter.check(&tick), true);
    }

    #[test]
    fn new_window_resets_and_never_rejects_first_message() {
        let tick = Arc::new(AtomicU64::new(0));
        let mut limiter = ConnectionRateLimiter::new(1);
        assert_eq!(limiter.check(&tick), false); // msg1, ventana 0
        assert_eq!(limiter.check(&tick), true); // msg2, ventana 0, excede limit=1
        tick.store(1, Ordering::Relaxed); // nueva ventana
        assert_eq!(limiter.check(&tick), false); // primer msg de ventana nueva, nunca rechaza
    }

    #[test]
    fn rejection_first_happens_at_message_limit_plus_one() {
        let tick = Arc::new(AtomicU64::new(0));
        let mut limiter = ConnectionRateLimiter::new(100);
        // mensajes 1..=100 deben pasar (el primero resetea y siempre
        // permite; count llega hasta 100 sin superar el límite)
        for _ in 0..100 {
            assert_eq!(limiter.check(&tick), false);
        }
        // mensaje 101: count=101 > 100 -> primer rechazo
        assert_eq!(limiter.check(&tick), true);
    }
}
