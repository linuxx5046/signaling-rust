//! Criterio de aceptación #7 y parte del #9 (especificacion-compatibilidad-v1.md
//! §10): nombres/tipos exactos de las métricas de la sección 6, más las
//! correcciones de las decisiones 9.2 (outgoing_messages sin doble conteo)
//! y 9.3 (subscribe_messages declarada pero siempre en 0), y exposición de
//! las métricas de proceso de la sección 6.1.
//!
//! El comportamiento de 9.1 (housekeeping limpia registros huérfanos) se
//! prueba de punta a punta en tests/housekeeping_timeout.rs, no acá.

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

async fn recv_text(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> String {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => return t,
            Ok(Some(Ok(_))) => continue,
            other => panic!("conexión cerrada o timeout inesperado: {other:?}"),
        }
    }
}

async fn fetch_metrics(http_port: u16) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", http_port)).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    // separar headers del body (el formato de texto de Prometheus)
    text.split("\r\n\r\n").nth(1).unwrap_or("").to_string()
}

/// Extrae el valor numérico de una línea de métrica sin labels, por
/// ejemplo `signaling_server_outgoing_messages 3` -> 3.0. Ignora líneas
/// de comentario (`# HELP` / `# TYPE`).
fn metric_value(body: &str, metric_name: &str) -> f64 {
    body.lines()
        .find(|line| {
            !line.starts_with('#')
                && (line == &metric_name || line.starts_with(&format!("{metric_name} ")))
        })
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("métrica {metric_name} no encontrada en:\n{body}"))
}

fn assert_type(body: &str, metric_name: &str, expected_type: &str) {
    let expected_line = format!("# TYPE {metric_name} {expected_type}");
    assert!(
        body.lines().any(|l| l == expected_line),
        "se esperaba la línea \"{expected_line}\" en /metrics, no se encontró. Cuerpo:\n{body}"
    );
}

#[tokio::test]
async fn custom_metric_names_and_types_match_spec_section_6() {
    let (_redis_guard, redis_port) = spawn_test_redis(25437);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14600u16;
    let http_port = 14601u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;
    wait_for_port(http_port).await;

    let body = fetch_metrics(http_port).await;

    assert_type(&body, "signaling_server_connected_clients", "gauge");
    assert_type(&body, "signaling_server_incoming_messages", "counter");
    assert_type(&body, "signaling_server_outgoing_messages", "counter");
    assert_type(&body, "signaling_server_publish_messages", "counter");
    assert_type(&body, "signaling_server_subscribe_messages", "counter");
    assert_type(&body, "signaling_server_redis_get_time", "gauge");
    assert_type(&body, "signaling_server_redis_set_time", "gauge");
    assert_type(&body, "signaling_server_redis_delete_time", "gauge");
    assert_type(&body, "signaling_server_redis_publish_time", "gauge");
    assert_type(&body, "signaling_server_redis_subscribe_time", "gauge");

    // decisión 9.3: declarada, nunca incrementada, siempre 0 (todavía sin
    // tráfico en este punto del test).
    assert_eq!(metric_value(&body, "signaling_server_subscribe_messages"), 0.0);
}

#[tokio::test]
async fn process_metrics_from_section_6_1_are_exposed() {
    let (_redis_guard, redis_port) = spawn_test_redis(25442);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14610u16;
    let http_port = 14611u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;
    wait_for_port(http_port).await;

    let body = fetch_metrics(http_port).await;

    // nombres estándar de Prometheus (sin prefijo signaling_server_),
    // vía prometheus::process_collector::ProcessCollector::for_self().
    for (name, kind) in [
        ("process_resident_memory_bytes", "gauge"),
        ("process_virtual_memory_bytes", "gauge"),
        ("process_cpu_seconds_total", "counter"),
        ("process_open_fds", "gauge"),
        ("process_max_fds", "gauge"),
        ("process_start_time_seconds", "gauge"),
        ("process_threads", "gauge"),
    ] {
        assert_type(&body, name, kind);
        let value = metric_value(&body, name);
        assert!(
            value >= 0.0,
            "{name} debería tener un valor numérico no negativo, se obtuvo {value}"
        );
    }

    // coherencia mínima: RSS real de un proceso recién arrancado debe ser
    // positivo (un binario de Rust con tokio no arranca con 0 bytes de RSS).
    let rss = metric_value(&body, "process_resident_memory_bytes");
    assert!(rss > 0.0, "RSS debería ser > 0, se obtuvo {rss}");

    let max_fds = metric_value(&body, "process_max_fds");
    let open_fds = metric_value(&body, "process_open_fds");
    assert!(
        open_fds <= max_fds,
        "process_open_fds ({open_fds}) no debería superar process_max_fds ({max_fds})"
    );
}

#[tokio::test]
async fn outgoing_messages_counts_once_per_real_send_not_twice() {
    // Decisión 9.2: el original incrementaba outgoing_messages dos veces
    // para mensajes que llegaban vía Redis (remoteData). Acá cada envío
    // real por el WS (send_outbound/send_raw) incrementa exactamente una
    // vez -- lo verificamos contando deltas exactos alrededor de un
    // intercambio conocido.
    let (_redis_guard, redis_port) = spawn_test_redis(25443);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14620u16;
    let http_port = 14621u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;
    wait_for_port(http_port).await;

    let connection_id = random_connection_id();
    let mut client1 = connect_client(port, &connection_id, "wallet", "extension").await;
    // apertura de client1 sola: 1 solo envío saliente no ocurre (nadie
    // más conectado todavía, sección 2.2: la lista de targetIds está
    // vacía, no se manda nada). outgoing_messages debe seguir en 0.
    let body = fetch_metrics(http_port).await;
    assert_eq!(metric_value(&body, "signaling_server_outgoing_messages"), 0.0);

    let mut client2 = connect_client(port, &connection_id, "extension", "wallet").await;
    // la apertura de client2 genera exactamente 2 envíos salientes:
    // remoteClientIsAlreadyConnected directo a client2, y
    // remoteClientJustConnected vía Redis a client1 (sección 2.2 §4).
    let _ = recv_text(&mut client1).await;
    let _ = recv_text(&mut client2).await;

    let body = fetch_metrics(http_port).await;
    assert_eq!(
        metric_value(&body, "signaling_server_outgoing_messages"),
        2.0,
        "apertura de client2 debe sumar exactamente 2 envíos salientes (ni más ni menos)"
    );

    // Un mensaje válido entre pares genera exactamente 2 envíos salientes
    // más: confirmation (a quien manda) + remoteData (a quien recibe, vía
    // Redis) -- NUNCA 4, que sería el bug del doble conteo original. Para
    // esto necesitamos el websocketId real del destinatario, que no se
    // guardó del par client1/client2 (ya se consumió con recv_text), así
    // que abrimos un segundo par de clientes dedicado a este delta.
    let _ = &connection_id; // (client1/client2 solo sirvieron para el chequeo de 0 y 2 anteriores)
    let connection_id_2 = random_connection_id();
    let mut client3 = connect_client(port, &connection_id_2, "wallet", "extension").await;
    let mut client4 = connect_client(port, &connection_id_2, "extension", "wallet").await;
    let open3 = recv_text(&mut client3).await;
    let open4 = recv_text(&mut client4).await;
    let open3: serde_json::Value = serde_json::from_str(&open3).unwrap();
    let open4: serde_json::Value = serde_json::from_str(&open4).unwrap();
    let client3_target = open3["remoteClientId"].as_str().unwrap().to_string();

    let before = metric_value(&fetch_metrics(http_port).await, "signaling_server_outgoing_messages");

    let msg = json!({
        "requestId": "req-delta",
        "method": "offer",
        "encryptedPayload": "deadbeef",
        "targetClientId": client3_target,
    });
    client3
        .send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string()))
        .await
        .unwrap();
    let _confirmation = recv_text(&mut client3).await;
    let _remote_data = recv_text(&mut client4).await;
    let _ = open4;

    let after = metric_value(&fetch_metrics(http_port).await, "signaling_server_outgoing_messages");
    assert_eq!(
        after - before,
        2.0,
        "un mensaje válido debe sumar exactamente 2 envíos salientes (confirmation + remoteData), nunca 4 (bug 9.2 del original)"
    );
}
