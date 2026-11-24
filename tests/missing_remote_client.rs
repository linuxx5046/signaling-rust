//! Criterio de aceptación #4 (especificacion-compatibilidad-v1.md §10):
//! `missingRemoteClientError` cuando `targetClientId` no es miembro
//! actual del Set `targetKey` de la conexión (sección 2.3 §5 de la spec).
//!
//! Cubre dos casos:
//! 1. Nadie del rol contrario está conectado todavía -> cualquier
//!    targetClientId falla.
//! 2. Alguien SÍ está conectado, pero el targetClientId usado no es el
//!    suyo (un UUID inventado) -> también debe fallar, y no debe
//!    mandarse `confirmation`.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
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

fn spawn_server(port: u16, http_port: u16, redis_port: u16) -> ServerGuard {
    let exe = env!("CARGO_BIN_EXE_signaling-rust");
    let child = Command::new(exe)
        .env("REDIS_HOST_PUB", "127.0.0.1")
        .env("REDIS_HOST_SUBS", "127.0.0.1")
        .env("REDIS_PORT", redis_port.to_string())
        .env("LOG_LEVEL", "4")
        .env("PORT", port.to_string())
        .env("HTTP_PORT", http_port.to_string())
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

fn make_message(method: &str, target_client_id: &str) -> Value {
    json!({
        "requestId": uuid::Uuid::new_v4().to_string(),
        "method": method,
        "encryptedPayload": "deadbeef",
        "targetClientId": target_client_id,
    })
}

async fn recv_json(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                return serde_json::from_str(&t).expect("respuesta no es JSON válido");
            }
            Ok(Some(Ok(_))) => continue,
            other => panic!("conexión cerrada o timeout inesperado: {other:?}"),
        }
    }
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

#[tokio::test]
async fn missing_remote_client_when_nobody_connected() {
    let (_redis_guard, redis_port) = spawn_test_redis(25434);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14300u16;
    let http_port = 14301u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;

    let connection_id = random_connection_id();
    let mut client = connect_client(port, &connection_id, "wallet", "extension").await;

    let msg = make_message("offer", &uuid::Uuid::new_v4().to_string());
    let request_id = msg["requestId"].as_str().unwrap().to_string();
    client
        .send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string()))
        .await
        .unwrap();

    let response = recv_json(&mut client).await;
    assert_eq!(response["info"], "missingRemoteClientError");
    assert_eq!(response["requestId"], request_id);
    // no debe haber más campos que info/requestId en este mensaje
    // (sección 3.2 de la spec).
    assert_eq!(
        response.as_object().unwrap().len(),
        2,
        "missingRemoteClientError solo debe tener info+requestId, se obtuvo: {response}"
    );
}

#[tokio::test]
async fn missing_remote_client_when_wrong_id_but_someone_connected() {
    let (_redis_guard, redis_port) = spawn_test_redis(25440);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14310u16;
    let http_port = 14311u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;

    let connection_id = random_connection_id();
    let mut client1 = connect_client(port, &connection_id, "wallet", "extension").await;
    let mut client2 = connect_client(port, &connection_id, "extension", "wallet").await;

    // drenar las notificaciones de apertura de ambos (remoteClientJustConnected /
    // remoteClientIsAlreadyConnected, no son relevantes para esta prueba).
    let _ = recv_json(&mut client1).await;
    let _ = recv_json(&mut client2).await;

    // client1 manda a un targetClientId inventado, que NO es el real
    // websocketId de client2 -- debe fallar igual que si nadie estuviera
    // conectado.
    let fake_id = uuid::Uuid::new_v4().to_string();
    let msg = make_message("offer", &fake_id);
    let request_id = msg["requestId"].as_str().unwrap().to_string();
    client1
        .send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string()))
        .await
        .unwrap();

    let response = recv_json(&mut client1).await;
    assert_eq!(response["info"], "missingRemoteClientError");
    assert_eq!(response["requestId"], request_id);

    // client2 no debe haber recibido nada (ni remoteData ni confirmation
    // le llegan a client1 tampoco) -- confirmamos que no hay un mensaje
    // ya esperando en client2 dentro de un margen corto.
    let nothing_pending = tokio::time::timeout(Duration::from_millis(500), client2.next()).await;
    assert!(
        nothing_pending.is_err(),
        "client2 no debería haber recibido nada tras el missingRemoteClientError de client1"
    );
}
