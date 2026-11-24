//! Criterio de aceptación #3 (especificacion-compatibilidad-v1.md §10):
//! rate limit en el borde exacto, a nivel de conexión WS real (no solo el
//! unit test del algoritmo en src/rate_limit.rs).
//!
//! Algoritmo (sección 7 de la spec): el primer mensaje de cada ventana
//! nunca se rechaza; el límite efectivo real es `RATE_LIMIT_MESSAGES + 1`
//! antes del primer rechazo; al exceder, el servidor cierra con code 1013.

use futures_util::{SinkExt, StreamExt};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn random_connection_id() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

async fn wait_for_port(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("el puerto {port} nunca quedó disponible");
}

fn spawn_server(port: u16, http_port: u16, redis_port: u16, rate_limit_messages: u32) -> ServerGuard {
    let exe = env!("CARGO_BIN_EXE_signaling-rust");
    let child = Command::new(exe)
        .env("REDIS_HOST_PUB", "127.0.0.1")
        .env("REDIS_HOST_SUBS", "127.0.0.1")
        .env("REDIS_PORT", redis_port.to_string())
        .env("LOG_LEVEL", "4")
        .env("PORT", port.to_string())
        .env("HTTP_PORT", http_port.to_string())
        // ventana larga (10s) para que todos los mensajes de la prueba
        // caigan dentro de la MISMA ventana -- lo que importa acá es el
        // límite dentro de una ventana, no la rotación entre ventanas
        // (eso ya lo cubre el unit test de src/rate_limit.rs).
        .env("RATE_LIMIT_MS", "10000")
        .env("RATE_LIMIT_MESSAGES", rate_limit_messages.to_string())
        // heartbeat largo para que el ping/pong de housekeeping no
        // interfiera con esta prueba.
        .env("HEARTBEAT_INTERVAL_MS", "60000")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("no se pudo lanzar signaling-rust");
    ServerGuard(child)
}

async fn connect_client(
    port: u16,
    connection_id: &str,
    target: &str,
    source: &str,
) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    let url = format!("ws://127.0.0.1:{port}/{connection_id}?target={target}&source={source}");
    let (ws, _) = connect_async(url).await.expect("no se pudo conectar");
    ws
}

fn spawn_test_redis(port: u16) -> (ServerGuard, u16) {
    let child = Command::new("redis-server")
        .arg("--port")
        .arg(port.to_string())
        .arg("--daemonize")
        .arg("no")
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("no se pudo lanzar redis-server (¿está instalado?)");
    (ServerGuard(child), port)
}

/// Lee mensajes hasta encontrar un `Close` (ignora texto normal, que acá
/// son `invalidMessageError` porque mandamos texto que no es JSON --
/// el rate limit se evalúa ANTES de parsear el mensaje, sección 2.3 §1,
/// así que no necesitamos JSON válido para esta prueba).
async fn wait_for_close(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> tokio_tungstenite::tungstenite::protocol::CloseFrame<'static> {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(Some(frame))))) => {
                return frame;
            }
            Ok(Some(Ok(_))) => continue,
            other => panic!("se esperaba un Close frame, se obtuvo: {other:?}"),
        }
    }
}

#[tokio::test]
async fn rate_limit_rejects_at_limit_plus_one() {
    let redis_port = 25433u16;
    let (_redis_guard, redis_port) = spawn_test_redis(redis_port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14200u16;
    let http_port = 14201u16;
    let limit = 5u32;
    let _server = spawn_server(port, http_port, redis_port, limit);
    wait_for_port(port).await;

    let connection_id = random_connection_id();
    let mut client = connect_client(port, &connection_id, "wallet", "extension").await;

    // Mensajes 1..=limit deben pasar (cada uno dispara invalidMessageError
    // porque el payload no es JSON, pero la conexión sigue abierta).
    for i in 1..=limit {
        client
            .send(tokio_tungstenite::tungstenite::Message::Text(format!(
                "no es json #{i}"
            )))
            .await
            .expect(&format!("el mensaje #{i} (dentro del límite) no debería fallar al enviar"));
        // drenar la respuesta (invalidMessageError) para no confundirla
        // con el close frame que buscamos después.
        match tokio::time::timeout(Duration::from_secs(5), client.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(_)))) => {}
            other => panic!("mensaje #{i}: se esperaba una respuesta de texto, se obtuvo {other:?}"),
        }
    }

    // Mensaje limit+1: este SÍ debe rechazarse (count = limit+1 > limit).
    client
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "este debe exceder el rate limit".to_string(),
        ))
        .await
        .expect("el envío en sí no debería fallar, el rechazo lo hace el servidor");

    let frame = wait_for_close(&mut client).await;
    assert_eq!(u16::from(frame.code), 1013, "código de cierre esperado 1013");
    assert_eq!(
        frame.reason.as_ref(),
        "rate limit hit, slow down",
        "razón de cierre esperada exacta (sección 7 de la spec)"
    );
}

#[tokio::test]
async fn first_message_within_limit_is_never_the_one_that_triggers_close() {
    // Variante con límite=1: msg1 nunca se rechaza, msg2 sí.
    let redis_port = 25439u16;
    let (_redis_guard, redis_port) = spawn_test_redis(redis_port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14210u16;
    let http_port = 14211u16;
    let limit = 1u32;
    let _server = spawn_server(port, http_port, redis_port, limit);
    wait_for_port(port).await;

    let connection_id = random_connection_id();
    let mut client = connect_client(port, &connection_id, "wallet", "extension").await;

    client
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "msg1 (primero de la ventana, nunca se rechaza)".to_string(),
        ))
        .await
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(5), client.next()).await {
        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["info"], "invalidMessageError", "msg1 no debió cerrarse, solo responder");
        }
        other => panic!("msg1: se esperaba respuesta de texto, se obtuvo {other:?}"),
    }

    client
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "msg2 (segundo de la misma ventana, excede limit=1)".to_string(),
        ))
        .await
        .unwrap();
    let frame = wait_for_close(&mut client).await;
    assert_eq!(u16::from(frame.code), 1013);
}
