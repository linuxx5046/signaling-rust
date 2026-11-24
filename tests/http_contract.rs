//! Criterio de aceptación #6 (especificacion-compatibilidad-v1.md §10):
//! `/health` y `/metrics` responden 200 para CUALQUIER método HTTP
//! (sección 5 de la spec: "cualquiera" en la columna de métodos
//! aceptados), y cualquier otra ruta responde 404.

use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::net::TcpStream;

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
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

/// Hace una petición HTTP cruda con el método dado (incluyendo métodos
/// no estándar), usando reqwest-less raw TCP para no necesitar una
/// dependencia HTTP cliente nueva en Cargo.toml.
async fn raw_http_request(http_port: u16, method: &str, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(("127.0.0.1", http_port))
        .await
        .expect("no se pudo conectar al puerto HTTP");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();

    let status_line = text.lines().next().unwrap_or("");
    // "HTTP/1.1 200 OK" -> 200
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status_code, text)
}

#[tokio::test]
async fn health_and_metrics_accept_any_http_method() {
    let (_redis_guard, redis_port) = spawn_test_redis(25436);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14500u16;
    let http_port = 14501u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;
    wait_for_port(http_port).await;

    for method in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"] {
        let (status, body) = raw_http_request(http_port, method, "/health").await;
        assert_eq!(status, 200, "/health con método {method} debería responder 200, cuerpo: {body}");
        if method != "HEAD" {
            assert!(
                body.contains("ok"),
                "/health con método {method} debería responder \"ok\", cuerpo: {body}"
            );
        }

        let (status, body) = raw_http_request(http_port, method, "/metrics").await;
        assert_eq!(status, 200, "/metrics con método {method} debería responder 200, cuerpo: {body}");
        if method != "HEAD" {
            assert!(
                body.contains("signaling_server_connected_clients"),
                "/metrics con método {method} debería incluir las métricas custom, cuerpo: {body}"
            );
        }
    }
}

#[tokio::test]
async fn any_other_route_is_404() {
    let (_redis_guard, redis_port) = spawn_test_redis(25441);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let port = 14510u16;
    let http_port = 14511u16;
    let _server = spawn_server(port, http_port, redis_port);
    wait_for_port(port).await;
    wait_for_port(http_port).await;

    for (method, path) in [
        ("GET", "/no-existe"),
        ("POST", "/"),
        ("GET", "/health/extra"),
        ("DELETE", "/metrics/extra"),
    ] {
        let (status, body) = raw_http_request(http_port, method, path).await;
        assert_eq!(
            status, 404,
            "{method} {path} debería responder 404, cuerpo: {body}"
        );
    }
}
