//! Criterio de aceptación #8 (especificacion-compatibilidad-v1.md §10) y
//! decisión 9.1: una conexión que deja de responder (sin cerrar limpio)
//! debe, tras el timeout de ping/pong, limpiarse de Redis y notificar
//! `remoteClientDisconnected` a la contraparte -- exactamente el mismo
//! flujo que un `close` normal (sección 2.4/2.5 de la spec).
//!
//! Para simular una conexión "muerta" sin cerrarla realmente: una vez
//! abierta, dejamos de leer (`.next()`) de su lado del WebSocket. Como
//! tokio-tungstenite solo genera el Pong automático cuando efectivamente
//! procesa un frame Ping entrante (vía `.next()`/`.read()`), una conexión
//! que simplemente deja de sondear su stream no responde a los Pings del
//! servidor -- aunque el socket TCP siga técnicamente abierto.

use futures_util::StreamExt;
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

fn spawn_server(port: u16, http_port: u16, redis_port: u16, heartbeat_ms: u64) -> ServerGuard {
    let exe = env!("CARGO_BIN_EXE_signaling-rust");
    let child = Command::new(exe)
        .env("REDIS_HOST_PUB", "127.0.0.1")
        .env("REDIS_HOST_SUBS", "127.0.0.1")
        .env("REDIS_PORT", redis_port.to_string())
        .env("LOG_LEVEL", "4")
        .env("PORT", port.to_string())
        .env("HTTP_PORT", http_port.to_string())
        .env("HEARTBEAT_INTERVAL_MS", heartbeat_ms.to_string())
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

async fn recv_json(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> serde_json::Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                return serde_json::from_str(&t).expect("respuesta no es JSON válido");
            }
            Ok(Some(Ok(_))) => continue,
            other => panic!("conexión cerrada o timeout inesperado esperando remoteClientDisconnected: {other:?}"),
        }
    }
}

#[tokio::test]
async fn dead_connection_is_cleaned_up_after_ping_pong_timeout() {
    let (_redis_guard, redis_port) = spawn_test_redis(25438);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14700u16;
    let http_port = 14701u16;
    // intervalo corto para que la prueba no tarde minutos: el servidor
    // detecta la ausencia de pong recién en el SEGUNDO tick sin
    // respuesta (ve sección 2.5 / src/ws/connection.rs), así que el
    // tiempo total esperado de detección es ~2x este valor.
    let heartbeat_ms = 300u64;
    let _server = spawn_server(port, http_port, redis_port, heartbeat_ms);
    wait_for_port(port).await;

    let connection_id = random_connection_id();

    // client_dead: se abre y nunca más se lee de su stream -- simula una
    // conexión que dejó de responder sin cerrar limpio.
    let mut client_dead = connect_client(port, &connection_id, "wallet", "extension").await;
    let mut client_observer = connect_client(port, &connection_id, "extension", "wallet").await;

    // drenar las notificaciones de apertura de ambos; la que le toque a
    // client_observer (sea remoteClientJustConnected o
    // remoteClientIsAlreadyConnected) nos da el websocketId real de
    // client_dead, que necesitamos para las aserciones de abajo.
    let open_dead = recv_json(&mut client_dead).await;
    let open_observer = recv_json(&mut client_observer).await;
    let _ = open_dead;
    let dead_id = open_observer["remoteClientId"]
        .as_str()
        .expect("client_observer debería conocer el websocketId de client_dead al abrir")
        .to_string();

    // a partir de acá, deliberadamente NO volvemos a hacer
    // client_dead.next().await -- client_dead se mantiene viva en scope
    // (el socket TCP sigue abierto) pero deja de procesar frames, así que
    // nunca contesta los Ping del servidor con un Pong.

    let disconnect_notice = tokio::time::timeout(
        Duration::from_millis(heartbeat_ms * 6),
        recv_json(&mut client_observer),
    )
    .await
    .expect("client_observer debería recibir remoteClientDisconnected tras el timeout de housekeeping");

    assert_eq!(disconnect_notice["info"], "remoteClientDisconnected");
    assert_eq!(disconnect_notice["remoteClientId"], dead_id);
    assert_eq!(
        disconnect_notice.as_object().unwrap().len(),
        2,
        "remoteClientDisconnected solo debe tener info+remoteClientId"
    );

    // Verificación directa en Redis (decisión 9.1 -- corregido respecto al
    // original: el registro no debe sobrevivir hasta el TTL de 12h, debe
    // desaparecer apenas el housekeeping detecta la conexión muerta):
    // registryKey de client_dead = "{connectionId}:wallet" (fue el `source`
    // que usamos al conectarla).
    let registry_key = format!("{connection_id}:wallet");
    let redis_client = ::redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))
        .expect("no se pudo construir el cliente redis de verificación");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("no se pudo conectar a redis para verificación");
    use ::redis::AsyncCommands;
    let is_member: bool = conn
        .sismember(&registry_key, &dead_id)
        .await
        .expect("fallo el SISMEMBER de verificación");
    assert!(
        !is_member,
        "tras el housekeeping, el websocketId de la conexión muerta ya no debería estar en {registry_key}"
    );

    // mantener client_dead viva hasta acá explícitamente, para que el
    // drop no ocurra antes de que el servidor haya tenido oportunidad de
    // detectar el timeout por sí mismo (y no por un cierre del lado del
    // cliente).
    drop(client_dead);
}
