//! Equivalente a `sdp-ice-exchange.test.ts` del original: simula un
//! intercambio real de SDP/ICE entre dos clientes conectados a DOS
//! instancias distintas de `signaling-rust`, coordinadas por Redis --
//! exactamente el caso cross-instance que el original prueba (puertos
//! 4000/4100 en el original).
//!
//! Levanta los procesos reales (el binario compilado), no llama a
//! funciones internas -- es una prueba de caja negra sobre el protocolo
//! observable, que es lo que importa para "compatibilidad".

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
    // 64 chars hex válidos, no necesita ser un SHA-256 real de nada en
    // particular -- el servidor solo valida la forma (sección 2.2 de la
    // spec), igual que el original.
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
        .env("LOG_LEVEL", "0")
        .env("PORT", port.to_string())
        .env("HTTP_PORT", http_port.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
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
        match ws.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                return serde_json::from_str(&t).expect("respuesta no es JSON válido");
            }
            Some(Ok(_)) => continue, // ping/pong/etc, ignorar
            other => panic!("conexión cerrada inesperadamente: {other:?}"),
        }
    }
}

/// Encuentra, entre varios Redis locales típicos, un puerto libre para
/// levantar un redis-server temporal dedicado a este test.
fn spawn_test_redis() -> (ServerGuard, u16) {
    let port = 25432u16; // puerto alto, poco probable que choque
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
async fn sdp_ice_exchange_cross_instance() {
    let (_redis_guard, redis_port) = spawn_test_redis();
    // darle tiempo a redis para levantar antes de que los servers intenten conectar
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port1 = 14000u16;
    let http1 = 14001u16;
    let port2 = 14100u16;
    let http2 = 14101u16;

    let _server1 = spawn_server(port1, http1, redis_port);
    let _server2 = spawn_server(port2, http2, redis_port);
    wait_for_port(port1).await;
    wait_for_port(port2).await;

    let connection_id = random_connection_id();

    // client1 en la instancia 1: target=wallet, source=extension
    // client2 en la instancia 2: target=extension, source=wallet
    // (misma topología que el original)
    let mut client1 = connect_client(port1, &connection_id, "wallet", "extension").await;
    let mut client2 = connect_client(port2, &connection_id, "extension", "wallet").await;

    // Ambos reciben la notificación de apertura del otro. El orden entre
    // client1 y client2 no está garantizado (cuál se conectó primero
    // decide quién manda "alreadyConnected" vs "justConnected"), así que
    // leemos de los dos y nos quedamos con el remoteClientId de cada uno,
    // sea cual sea el tipo de mensaje que les tocó.
    let open_msg1 = recv_json(&mut client1).await;
    let open_msg2 = recv_json(&mut client2).await;

    let client1_target = open_msg1["remoteClientId"]
        .as_str()
        .expect("client1 debería recibir un remoteClientId al abrir")
        .to_string();
    let client2_target = open_msg2["remoteClientId"]
        .as_str()
        .expect("client2 debería recibir un remoteClientId al abrir")
        .to_string();

    // client2 (wallet) manda offer + 3 iceCandidate a client1
    // -> debe dirigirse al ID de client1 visto por client2, es decir
    //    client2_target (y no client1_target, que es el ID de client2
    //    visto por client1 -- estaban cruzados).
    let mut sent_by_client2 = vec![];
    for method in ["offer", "iceCandidate", "iceCandidate", "iceCandidate"] {
        let msg = make_message(method, &client2_target);
        client2
            .send(tokio_tungstenite::tungstenite::Message::Text(
                msg.to_string(),
            ))
            .await
            .unwrap();
        sent_by_client2.push(msg);
    }

    // client1 (extension) manda answer + 3 iceCandidate a client2
    // -> debe dirigirse al ID de client2 visto por client1, es decir
    //    client1_target (mismo cruce que arriba, corregido).
    let mut sent_by_client1 = vec![];
    for method in ["answer", "iceCandidate", "iceCandidate", "iceCandidate"] {
        let msg = make_message(method, &client1_target);
        client1
            .send(tokio_tungstenite::tungstenite::Message::Text(
                msg.to_string(),
            ))
            .await
            .unwrap();
        sent_by_client1.push(msg);
    }

    // Cada cliente debe recibir 8 mensajes en total: 4 `confirmation` por
    // lo que mandó él mismo, y 4 `remoteData` por lo que le mandó la
    // contraparte (sección 2.3 de la spec: toda publicación exitosa
    // termina en confirmation para el emisor y remoteData para el
    // destinatario).
    let mut client1_received = vec![];
    for _ in 0..8 {
        client1_received.push(recv_json(&mut client1).await);
    }
    let mut client2_received = vec![];
    for _ in 0..8 {
        client2_received.push(recv_json(&mut client2).await);
    }

    // client1 recibió confirmation por cada uno de sus propios 4 envíos
    for sent in &sent_by_client1 {
        let request_id = sent["requestId"].as_str().unwrap();
        let found = client1_received
            .iter()
            .any(|m| m["info"] == "confirmation" && m["requestId"] == request_id);
        assert!(found, "client1 no recibió confirmation para {request_id}");
    }

    // client1 recibió remoteData por cada uno de los 4 envíos de client2
    for sent in &sent_by_client2 {
        let request_id = sent["requestId"].as_str().unwrap();
        let found = client1_received
            .iter()
            .any(|m| m["info"] == "remoteData" && m["requestId"] == request_id);
        assert!(found, "client1 no recibió remoteData para {request_id}");
    }

    // client2 recibió confirmation por cada uno de sus propios 4 envíos
    for sent in &sent_by_client2 {
        let request_id = sent["requestId"].as_str().unwrap();
        let found = client2_received
            .iter()
            .any(|m| m["info"] == "confirmation" && m["requestId"] == request_id);
        assert!(found, "client2 no recibió confirmation para {request_id}");
    }

    // client2 recibió remoteData por cada uno de los 4 envíos de client1
    for sent in &sent_by_client1 {
        let request_id = sent["requestId"].as_str().unwrap();
        let found = client2_received
            .iter()
            .any(|m| m["info"] == "remoteData" && m["requestId"] == request_id);
        assert!(found, "client2 no recibió remoteData para {request_id}");
    }
}
