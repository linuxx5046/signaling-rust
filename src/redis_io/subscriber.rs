//! Replica el diseño del original: UN solo cliente subscriber para todo el
//! proceso (sección 2.2 de `arquitectura-rust-v1.md`), con un router en
//! memoria que despacha cada mensaje entrante al buzón de la conexión
//! local correspondiente.

use dashmap::DashMap;
use futures_util::StreamExt;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, warn};

/// Mapea websocketId -> buzón de la conexión local. El payload que se
/// reenvía es el JSON crudo tal como llegó de Redis (ya serializado),
/// así la conexión solo tiene que escribirlo directo al WebSocket sin
/// tener que deserializar y volver a serializar.
pub type ChannelRouter = Arc<DashMap<String, mpsc::UnboundedSender<String>>>;

enum SubscriberCommand {
    Subscribe {
        channel: String,
        sender: mpsc::UnboundedSender<String>,
        ack: tokio::sync::oneshot::Sender<redis::RedisResult<()>>,
    },
    Unsubscribe {
        channel: String,
    },
}

#[derive(Clone)]
pub struct SubscriberHandle {
    control_tx: mpsc::UnboundedSender<SubscriberCommand>,
}

impl SubscriberHandle {
    /// Suscribe y espera a que el `SUBSCRIBE` real contra Redis termine,
    /// igual que el original espera el `Promise` de `subscribe()` dentro
    /// del `Promise.all` de `open` (sección 2.2 de la spec) -- así se evita
    /// una ventana donde un mensaje publicado a este canal llegue antes de
    /// que la suscripción esté realmente activa en Redis.
    pub async fn subscribe(
        &self,
        channel: String,
        sender: mpsc::UnboundedSender<String>,
        metrics: &crate::metrics::Metrics,
    ) -> redis::RedisResult<()> {
        let t0 = std::time::Instant::now();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let _ = self.control_tx.send(SubscriberCommand::Subscribe {
            channel,
            sender,
            ack: ack_tx,
        });
        let result = match ack_rx.await {
            Ok(result) => result,
            Err(_) => Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "subscriber task no respondió",
            ))),
        };
        metrics
            .redis_subscribe_time
            .set(t0.elapsed().as_secs_f64() * 1000.0);
        result
    }

    pub fn unsubscribe(&self, channel: String) {
        let _ = self
            .control_tx
            .send(SubscriberCommand::Unsubscribe { channel });
    }
}

/// Arranca la tarea única que posee la conexión PubSub de Redis y el
/// router. Devuelve un handle liviano (clonable) para que cada conexión
/// WS pida subscribe/unsubscribe sin tocar la conexión Redis directamente.
pub async fn spawn_subscriber(
    client: redis::Client,
) -> redis::RedisResult<SubscriberHandle> {
    let pubsub = client.get_async_pubsub().await?;
    let (mut sink, mut stream) = pubsub.split();

    let (control_tx, mut control_rx) = mpsc::unbounded_channel::<SubscriberCommand>();
    let router: ChannelRouter = Arc::new(DashMap::new());

    tokio::spawn({
        let router = router.clone();
        async move {
            loop {
                tokio::select! {
                    maybe_msg = stream.next() => {
                        match maybe_msg {
                            Some(msg) => {
                                let channel = msg.get_channel_name().to_string();
                                let payload: String = match msg.get_payload() {
                                    Ok(p) => p,
                                    Err(e) => {
                                        warn!("redis pubsub: payload no es string válido: {e}");
                                        continue;
                                    }
                                };
                                if let Some(sender) = router.get(&channel) {
                                    // Si el receptor ya cerró (la conexión WS
                                    // local murió entre el open y este punto),
                                    // simplemente se descarta -- no hay a quién
                                    // entregarlo, igual que en el original
                                    // cuando el callback de suscripción corre
                                    // sobre un socket ya cerrado.
                                    let _ = sender.send(payload);
                                }
                            }
                            None => {
                                error!("redis pubsub: stream de mensajes cerrado inesperadamente");
                                break;
                            }
                        }
                    }
                    cmd = control_rx.recv() => {
                        match cmd {
                            Some(SubscriberCommand::Subscribe { channel, sender, ack }) => {
                                router.insert(channel.clone(), sender);
                                let result = sink.subscribe(&channel).await;
                                if let Err(ref e) = result {
                                    error!("redis pubsub: fallo al suscribirse a {channel}: {e}");
                                    router.remove(&channel);
                                }
                                let _ = ack.send(result);
                            }
                            Some(SubscriberCommand::Unsubscribe { channel }) => {
                                router.remove(&channel);
                                if let Err(e) = sink.unsubscribe(&channel).await {
                                    error!("redis pubsub: fallo al desuscribirse de {channel}: {e}");
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        }
    });

    Ok(SubscriberHandle { control_tx })
}

/// `PUBLISH` a un canal específico (un websocketId). Usado para entregar
/// mensajes a una conexión que puede estar en esta instancia o en otra
/// (sección 4.2 de la especificación). Mide tiempo e incrementa el
/// contador de publicaciones, igual que `data/redis.ts` en el original.
pub async fn publish(
    conn: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    payload: &str,
    metrics: &crate::metrics::Metrics,
) -> redis::RedisResult<()> {
    let t0 = std::time::Instant::now();
    let _: () = conn.publish(channel, payload).await?;
    metrics
        .redis_publish_time
        .set(t0.elapsed().as_secs_f64() * 1000.0);
    metrics.publish_messages.inc();
    Ok(())
}
