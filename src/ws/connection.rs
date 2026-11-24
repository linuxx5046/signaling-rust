use crate::rate_limit::ConnectionRateLimiter;
use crate::redis_io::{commands, subscriber};
use crate::state::AppState;
use crate::ws::messages::OutboundMessage;
use crate::ws::validate::validate_message;
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message as WsMessage};
use tracing::{debug, error, warn};
use uuid::Uuid;

const ROLES: [&str; 2] = ["wallet", "extension"];

fn is_valid_sha256_hex(s: &str) -> bool {
    // Equivalente a /^[a-f0-9]{64}$/gi del original (sección 2.2 de la spec).
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Resultado de validar los parámetros de apertura (sección 2.2).
struct OpenParams {
    connection_id: String,
    target: String,
    source: String,
}

/// Replica el orden exacto de las 6 validaciones de `open` en la spec.
/// Devuelve el código/razón de cierre WS a usar si alguna falla.
fn validate_open_params(uri: &str) -> Result<OpenParams, (u16, &'static str)> {
    let parsed = match url::Url::parse(&format!("ws://placeholder{uri}")) {
        Ok(u) => u,
        Err(_) => return Err((1003, "missing connectionId in path")),
    };

    let connection_id = parsed.path().trim_start_matches('/').to_string();
    if !is_valid_sha256_hex(&connection_id) {
        return Err((1003, "missing connectionId in path"));
    }

    let query: std::collections::HashMap<String, String> =
        parsed.query_pairs().into_owned().collect();

    let target = match query.get("target") {
        None => return Err((1003, "missing target")),
        Some(t) => t.clone(),
    };
    if !ROLES.contains(&target.as_str()) {
        return Err((1003, "invalid target"));
    }

    let source = match query.get("source") {
        None => return Err((1003, "missing source")),
        Some(s) => s.clone(),
    };
    if !ROLES.contains(&source.as_str()) {
        return Err((1003, "invalid source"));
    }

    if source == target {
        return Err((1003, "source and target needs to be different"));
    }

    Ok(OpenParams {
        connection_id,
        target,
        source,
    })
}

/// Envía un `OutboundMessage` directo por el WS, serializándolo, e
/// incrementa `outgoing_messages` UNA sola vez (decisión 9.2: corrige el
/// doble conteo del original).
async fn send_outbound<S>(
    sink: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<S>,
        WsMessage,
    >,
    metrics: &crate::metrics::Metrics,
    msg: OutboundMessage,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let json = match serde_json::to_string(&msg) {
        Ok(j) => j,
        Err(e) => {
            error!("no se pudo serializar OutboundMessage: {e}");
            return;
        }
    };
    if sink.send(WsMessage::Text(json)).await.is_ok() {
        metrics.outgoing_messages.inc();
    }
}

/// Igual que `send_outbound` pero para payloads crudos que ya llegaron
/// serializados (típicamente, reenviados desde Redis pub/sub).
async fn send_raw<S>(
    sink: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<S>,
        WsMessage,
    >,
    metrics: &crate::metrics::Metrics,
    payload: String,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if sink.send(WsMessage::Text(payload)).await.is_ok() {
        metrics.outgoing_messages.inc();
    }
}

pub async fn handle_connection(stream: TcpStream, state: Arc<AppState>) {
    let uri_holder: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let uri_holder_cb = uri_holder.clone();
    let callback = move |req: &Request, response: Response| {
        *uri_holder_cb.lock().unwrap() = Some(req.uri().to_string());
        Ok(response)
    };

    let ws_stream = match tokio_tungstenite::accept_hdr_async(stream, callback).await {
        Ok(s) => s,
        Err(e) => {
            debug!("ws handshake falló: {e}");
            return;
        }
    };

    let uri_str = uri_holder.lock().unwrap().clone().unwrap_or_default();
    let (mut ws_sink, mut ws_rx) = ws_stream.split();

    let open_params = match validate_open_params(&uri_str) {
        Ok(p) => p,
        Err((code, reason)) => {
            let _ = ws_sink
                .send(WsMessage::Close(Some(CloseFrame {
                    code: CloseCode::from(code),
                    reason: reason.into(),
                })))
                .await;
            return;
        }
    };

    let websocket_id = Uuid::new_v4().to_string();
    let registry_key = format!("{}:{}", open_params.connection_id, open_params.source);
    let target_key = format!("{}:{}", open_params.connection_id, open_params.target);

    let mut redis_conn = state.redis_conn.clone();
    // `MultiplexedConnection` es un handle liviano y clonable (todas las
    // clonas comparten la misma conexión TCP real, multiplexada) -- se
    // clona una vez más para poder usarla dos veces en paralelo dentro
    // del mismo `tokio::join!` sin pelear por el borrow mutable.
    let mut redis_conn_2 = redis_conn.clone();
    let (mailbox_tx, mut mailbox_rx) = mpsc::unbounded_channel::<String>();

    // Las 3 operaciones del Promise.all original: subscribe, setData,
    // getTargetWebsocketIds -- en paralelo (sección 2.2 de la spec).
    let (subscribe_result, register_result, target_ids_result) = tokio::join!(
        state.subscriber.subscribe(websocket_id.clone(), mailbox_tx.clone(), &state.metrics),
        commands::register(&mut redis_conn, &registry_key, &websocket_id, &state.metrics),
        commands::get_target_ids(&mut redis_conn_2, &target_key, &state.metrics),
    );

    if let Err(e) = subscribe_result {
        error!("fallo al suscribirse en open: {e}");
        return;
    }
    if let Err(e) = register_result {
        error!("fallo al registrar en open: {e}");
        return;
    }
    let target_ids = target_ids_result.unwrap_or_default();

    state.metrics.connected_clients.inc();

    for target_id in &target_ids {
        send_outbound(
            &mut ws_sink,
            &state.metrics,
            OutboundMessage::RemoteClientIsAlreadyConnected {
                remote_client_id: target_id.clone(),
            },
        )
        .await;

        let outbound = OutboundMessage::RemoteClientJustConnected {
            remote_client_id: websocket_id.clone(),
        };
        if let Ok(payload) = serde_json::to_string(&outbound) {
            let _ = subscriber::publish(&mut redis_conn, target_id, &payload, &state.metrics).await;
        }
    }

    let mut limiter = ConnectionRateLimiter::new(state.config.rate_limit_messages);
    let mut ping_interval =
        tokio::time::interval(Duration::from_millis(state.config.heartbeat_interval_ms));
    ping_interval.tick().await; // el primer tick es inmediato; lo consumimos
    let mut awaiting_pong = false;

    loop {
        tokio::select! {
            incoming = ws_rx.next() => {
                match incoming {
                    Some(Ok(WsMessage::Text(text))) => {
                        if limiter.check(&state.rate_limit_tick) {
                            let _ = ws_sink.send(WsMessage::Close(Some(CloseFrame {
                                code: CloseCode::from(1013),
                                reason: "rate limit hit, slow down".into(),
                            }))).await;
                            break;
                        }
                        state.metrics.incoming_messages.inc();
                        handle_text_message(
                            &text,
                            &mut ws_sink,
                            &mut redis_conn,
                            &target_key,
                            &websocket_id,
                            &state,
                        ).await;
                    }
                    Some(Ok(WsMessage::Ping(payload))) => {
                        let _ = ws_sink.send(WsMessage::Pong(payload)).await;
                    }
                    Some(Ok(WsMessage::Pong(_))) => {
                        awaiting_pong = false;
                    }
                    Some(Ok(WsMessage::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(_)) => {
                        // Binary/Frame -- el protocolo original no los usa,
                        // se ignoran.
                    }
                    Some(Err(e)) => {
                        warn!("error leyendo ws ({websocket_id}): {e}");
                        break;
                    }
                }
            }
            maybe_payload = mailbox_rx.recv() => {
                match maybe_payload {
                    Some(payload) => send_raw(&mut ws_sink, &state.metrics, payload).await,
                    None => break,
                }
            }
            _ = ping_interval.tick() => {
                if awaiting_pong {
                    debug!("housekeeping: {websocket_id} no respondió al ping, cerrando");
                    break;
                }
                if ws_sink.send(WsMessage::Ping(vec![])).await.is_err() {
                    break;
                }
                awaiting_pong = true;
            }
        }
    }

    // Cleanup -- igual para cierre normal, error de lectura, o housekeeping
    // por ping/pong timeout (sección 2.4 / 2.5 de la spec).
    if let Ok(target_ids) = commands::get_target_ids(&mut redis_conn, &target_key, &state.metrics).await {
        let outbound = OutboundMessage::RemoteClientDisconnected {
            remote_client_id: websocket_id.clone(),
        };
        if let Ok(payload) = serde_json::to_string(&outbound) {
            for target_id in target_ids {
                let _ = subscriber::publish(&mut redis_conn, &target_id, &payload, &state.metrics).await;
            }
        }
    }
    state.subscriber.unsubscribe(websocket_id.clone());
    let _ = commands::deregister(&mut redis_conn, &registry_key, &websocket_id, &state.metrics).await;
    state.metrics.connected_clients.dec();
}

async fn handle_text_message<S>(
    text: &str,
    ws_sink: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, WsMessage>,
    redis_conn: &mut ::redis::aio::MultiplexedConnection,
    target_key: &str,
    own_websocket_id: &str,
    state: &Arc<AppState>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let parsed: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => {
            send_outbound(
                ws_sink,
                &state.metrics,
                OutboundMessage::InvalidMessageError {
                    error: "invalid message format, expected JSON".to_string(),
                    data: text.to_string(),
                },
            )
            .await;
            return;
        }
    };

    let request_id_if_present = parsed
        .get("requestId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let msg = match validate_message(&parsed) {
        Ok(m) => m,
        Err(issues) => {
            send_outbound(
                ws_sink,
                &state.metrics,
                OutboundMessage::ValidationError {
                    request_id: request_id_if_present,
                    error: issues,
                },
            )
            .await;
            return;
        }
    };

    let is_target_member = commands::is_member(redis_conn, target_key, &msg.target_client_id)
        .await
        .unwrap_or(false);

    if !is_target_member {
        send_outbound(
            ws_sink,
            &state.metrics,
            OutboundMessage::MissingRemoteClientError {
                request_id: msg.request_id.clone(),
            },
        )
        .await;
        return;
    }

    let outbound = OutboundMessage::RemoteData {
        request_id: msg.request_id.clone(),
        data: msg.clone(),
        remote_client_id: own_websocket_id.to_string(),
    };
    if let Ok(payload) = serde_json::to_string(&outbound) {
        let _ = subscriber::publish(redis_conn, &msg.target_client_id, &payload, &state.metrics).await;
    }

    send_outbound(
        ws_sink,
        &state.metrics,
        OutboundMessage::Confirmation {
            request_id: msg.request_id,
        },
    )
    .await;
}
