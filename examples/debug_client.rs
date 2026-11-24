use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() {
    let connection_id = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    println!("connection_id = {connection_id}");

    let url1 = format!("ws://127.0.0.1:19000/{connection_id}?target=wallet&source=extension");
    println!("conectando client1: {url1}");
    let (mut client1, _) = connect_async(url1).await.expect("client1 connect failed");
    println!("client1 conectado, sin segundo cliente todavia -- no debería recibir nada enseguida");

    // Enviar un mensaje con targetClientId inexistente, debería dar missingRemoteClientError
    let msg = serde_json::json!({
        "requestId": "req-1",
        "method": "offer",
        "encryptedPayload": "deadbeef",
        "targetClientId": "no-existe"
    });
    println!("mandando mensaje con target inexistente...");
    client1
        .send(Message::Text(msg.to_string()))
        .await
        .expect("send failed");

    println!("esperando respuesta...");
    match tokio::time::timeout(std::time::Duration::from_secs(5), client1.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => println!("recibido: {t}"),
        other => println!("timeout o error: {other:?}"),
    }

    println!("--- ahora conectando client2 con roles cruzados ---");
    let url2 = format!("ws://127.0.0.1:19000/{connection_id}?target=extension&source=wallet");
    let (mut client2, _) = connect_async(url2).await.expect("client2 connect failed");

    println!("esperando mensaje en client1 (debería ser remoteClientJustConnected)...");
    match tokio::time::timeout(std::time::Duration::from_secs(5), client1.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => println!("client1 recibió: {t}"),
        other => println!("client1 timeout o error: {other:?}"),
    }

    println!("esperando mensaje en client2 (debería ser remoteClientIsAlreadyConnected)...");
    match tokio::time::timeout(std::time::Duration::from_secs(5), client2.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => println!("client2 recibió: {t}"),
        other => println!("client2 timeout o error: {other:?}"),
    }

    println!("listo");
}
