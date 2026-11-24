use serde::{Deserialize, Serialize};

/// Los 4 métodos válidos del protocolo (sección 3.1 de la especificación).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Method {
    Offer,
    Answer,
    IceCandidate,
    IceCandidates,
}

/// Mensaje entrante (cliente -> servidor), ya validado.
/// `source` y `connectionId` son campos redundantes heredados: el original
/// los acepta si vienen pero no los usa para nada funcional. Se preservan
/// acá únicamente para poder reflejar el mensaje completo en `remoteData.data`,
/// igual que hace el original.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub request_id: String,
    pub method: Method,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    pub encrypted_payload: String,
    pub target_client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_at: Option<f64>,
}

// El validador manual (validate.rs) no deserializa directamente a este
// struct: primero inspecciona el JSON crudo como `serde_json::Value` para
// poder acumular TODOS los errores de campos faltantes/inválidos (serde
// aborta en el primero). Recién si la validación pasa se construye este
// struct tipado, que sí usa camelCase en el wire vía rename_all de arriba.

/// Un error de validación individual, en el estilo de los `issues` de zod:
/// qué campo falló y por qué. El original expone `ZodError[]`; esta forma
/// preserva la misma idea (lista de fallas, una por campo) sin atarse a la
/// forma exacta interna de zod, que no es parte del contrato observable por
/// los clientes (solo importa la cantidad y que sea una lista).
#[derive(Debug, Clone, Serialize)]
pub struct ValidationIssue {
    pub path: Vec<String>,
    pub message: String,
}

/// Los 8 tipos de mensaje salientes (servidor -> cliente), unión discriminada
/// por el campo `info`, literales exactos en camelCase (sección 3.2 de la spec).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "info")]
#[serde(rename_all = "camelCase")]
pub enum OutboundMessage {
    Confirmation {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    RemoteData {
        #[serde(rename = "requestId")]
        request_id: String,
        data: Message,
        #[serde(rename = "remoteClientId")]
        remote_client_id: String,
    },
    RemoteClientDisconnected {
        #[serde(rename = "remoteClientId")]
        remote_client_id: String,
    },
    RemoteClientIsAlreadyConnected {
        #[serde(rename = "remoteClientId")]
        remote_client_id: String,
    },
    RemoteClientJustConnected {
        #[serde(rename = "remoteClientId")]
        remote_client_id: String,
    },
    MissingRemoteClientError {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    InvalidMessageError {
        error: String,
        data: String,
    },
    ValidationError {
        #[serde(rename = "requestId", skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        error: Vec<ValidationIssue>,
    },
}
