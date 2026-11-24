//! Criterio de aceptación #5 (especificacion-compatibilidad-v1.md §10):
//! formato exacto de los 8 tipos de mensaje saliente (sección 3.2 de la
//! spec) -- no solo que el campo `info` tenga el literal correcto, sino
//! que el conjunto de campos presentes sea exactamente el esperado (ni
//! de más ni de menos).

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::BTreeSet;
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

fn keys_of(v: &Value) -> BTreeSet<String> {
    v.as_object()
        .expect("se esperaba un objeto JSON")
        .keys()
        .cloned()
        .collect()
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
async fn all_eight_outbound_message_shapes_are_exact() {
    let (_redis_guard, redis_port) = spawn_test_redis(25435);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14400u16;
    let http_port = 14401u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;

    let connection_id = random_connection_id();

    // --- 1 y 2: remoteClientJustConnected / remoteClientIsAlreadyConnected ---
    let mut client1 = connect_client(port, &connection_id, "wallet", "extension").await;
    let mut client2 = connect_client(port, &connection_id, "extension", "wallet").await;

    let open1 = recv_json(&mut client1).await;
    let open2 = recv_json(&mut client2).await;
    // uno de los dos es remoteClientIsAlreadyConnected (al que llegó
    // segundo se le avisa que el otro ya estaba) y el otro recibe, vía
    // Redis, remoteClientJustConnected -- no está garantizado el orden,
    // así que verificamos el conjunto de los dos tipos sin asumir cuál
    // le tocó a cada cliente.
    let mut seen_open_infos: Vec<String> = vec![
        open1["info"].as_str().unwrap().to_string(),
        open2["info"].as_str().unwrap().to_string(),
    ];
    seen_open_infos.sort();
    assert_eq!(
        seen_open_infos,
        vec!["remoteClientIsAlreadyConnected", "remoteClientJustConnected"]
    );
    for open_msg in [&open1, &open2] {
        assert_eq!(
            keys_of(open_msg),
            BTreeSet::from(["info".to_string(), "remoteClientId".to_string()])
        );
    }
    let client1_target = open1["remoteClientId"].as_str().unwrap().to_string();
    let client2_target = open2["remoteClientId"].as_str().unwrap().to_string();

    // --- 3: invalidMessageError (texto que no es JSON) ---
    client1
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "esto no es json".to_string(),
        ))
        .await
        .unwrap();
    let invalid = recv_json(&mut client1).await;
    assert_eq!(invalid["info"], "invalidMessageError");
    assert_eq!(invalid["data"], "esto no es json");
    assert_eq!(
        keys_of(&invalid),
        BTreeSet::from(["info".to_string(), "error".to_string(), "data".to_string()])
    );

    // --- 4: validationError (objeto vacío -> 4 errores, sección 3.1) ---
    client1
        .send(tokio_tungstenite::tungstenite::Message::Text(
            json!({}).to_string(),
        ))
        .await
        .unwrap();
    let validation = recv_json(&mut client1).await;
    assert_eq!(validation["info"], "validationError");
    assert!(validation["error"].is_array());
    assert_eq!(
        validation["error"].as_array().unwrap().len(),
        4,
        "un objeto vacío debe producir exactamente 4 errores (sección 3.1)"
    );
    // requestId puede faltar (estaba ausente en el objeto vacío) ->
    // skip_serializing_if, así que la clave ni debe aparecer.
    assert_eq!(
        keys_of(&validation),
        BTreeSet::from(["info".to_string(), "error".to_string()]),
        "sin requestId en el mensaje entrante, el campo no debe serializarse en absoluto"
    );

    // --- 5: missingRemoteClientError ---
    let fake_target = uuid::Uuid::new_v4().to_string();
    let bad_msg = json!({
        "requestId": "req-missing",
        "method": "offer",
        "encryptedPayload": "deadbeef",
        "targetClientId": fake_target,
    });
    client1
        .send(tokio_tungstenite::tungstenite::Message::Text(
            bad_msg.to_string(),
        ))
        .await
        .unwrap();
    let missing = recv_json(&mut client1).await;
    assert_eq!(missing["info"], "missingRemoteClientError");
    assert_eq!(missing["requestId"], "req-missing");
    assert_eq!(
        keys_of(&missing),
        BTreeSet::from(["info".to_string(), "requestId".to_string()])
    );

    // --- 6 y 7: confirmation (para quien manda) y remoteData (para quien recibe) ---
    let good_msg = json!({
        "requestId": "req-good",
        "method": "offer",
        "encryptedPayload": "deadbeef",
        "targetClientId": client1_target, // el ID de client2 visto por client1
    });
    client1
        .send(tokio_tungstenite::tungstenite::Message::Text(
            good_msg.to_string(),
        ))
        .await
        .unwrap();

    let confirmation = recv_json(&mut client1).await;
    assert_eq!(confirmation["info"], "confirmation");
    assert_eq!(confirmation["requestId"], "req-good");
    assert_eq!(
        keys_of(&confirmation),
        BTreeSet::from(["info".to_string(), "requestId".to_string()])
    );

    let remote_data = recv_json(&mut client2).await;
    assert_eq!(remote_data["info"], "remoteData");
    assert_eq!(remote_data["requestId"], "req-good");
    assert_eq!(remote_data["remoteClientId"], client2_target); // el ID de client1 visto por client2
    assert_eq!(remote_data["data"]["requestId"], "req-good");
    assert_eq!(remote_data["data"]["method"], "offer");
    assert_eq!(remote_data["data"]["encryptedPayload"], "deadbeef");
    assert_eq!(remote_data["data"]["targetClientId"], client1_target);
    assert_eq!(
        keys_of(&remote_data),
        BTreeSet::from([
            "info".to_string(),
            "requestId".to_string(),
            "data".to_string(),
            "remoteClientId".to_string(),
        ])
    );
    // el objeto `data` refleja el mensaje entrante completo: requestId,
    // method, encryptedPayload, targetClientId (source/connectionId/startAt
    // son opcionales y no vinieron en este mensaje, así que no deben
    // aparecer -- sección 3.1/3.2 de la spec).
    assert_eq!(
        keys_of(&remote_data["data"]),
        BTreeSet::from([
            "requestId".to_string(),
            "method".to_string(),
            "encryptedPayload".to_string(),
            "targetClientId".to_string(),
        ])
    );

    // --- 8: remoteClientDisconnected (cerrar client1 limpiamente y
    //        observar que client2 recibe el aviso) ---
    client1
        .close(None)
        .await
        .expect("cierre limpio de client1 no debería fallar");
    let disconnected = recv_json(&mut client2).await;
    assert_eq!(disconnected["info"], "remoteClientDisconnected");
    assert_eq!(disconnected["remoteClientId"], client2_target);
    assert_eq!(
        keys_of(&disconnected),
        BTreeSet::from(["info".to_string(), "remoteClientId".to_string()])
    );
}
