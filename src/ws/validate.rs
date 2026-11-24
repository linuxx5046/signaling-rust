//! Validación manual, campo por campo, acumulando errores.
//!
//! Por qué no un `#[derive(Deserialize)]` común: serde aborta en el primer
//! campo que falla. El test original (`validate.test.ts`) exige que un
//! objeto vacío produzca EXACTAMENTE 4 errores (uno por cada campo
//! requerido ausente: requestId, method, encryptedPayload, targetClientId).
//! Para igualar ese conteo hay que validar el objeto completo sin abortar
//! a mitad de camino.

use super::messages::{Message, Method, ValidationIssue};
use serde_json::Value;

/// Intenta validar un `Value` JSON crudo como `Message`.
/// Devuelve el mensaje tipado si es válido, o la lista completa de
/// issues encontrados (puede tener más de un issue por campo, igual
/// que zod, si por ejemplo falta un campo Y además hay otro problema
/// no relacionado).
pub fn validate_message(value: &Value) -> Result<Message, Vec<ValidationIssue>> {
    let mut issues: Vec<ValidationIssue> = Vec::new();

    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            // No es ni siquiera un objeto JSON: zod también fallaría acá,
            // pero en la práctica esto nunca ocurre porque el caller ya
            // parseó el frame completo como JSON antes de llegar hasta
            // acá (ver connection.rs, paso 3 de la sección 2.3 de la spec:
            // un fallo de parseo JSON es un error distinto, invalidMessageError,
            // no validationError).
            issues.push(ValidationIssue {
                path: vec![],
                message: "expected object".to_string(),
            });
            return Err(issues);
        }
    };

    // requestId: string, requerido
    let request_id = match obj.get("requestId") {
        None => {
            issues.push(required_issue("requestId"));
            None
        }
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            issues.push(type_issue("requestId", "string"));
            None
        }
    };

    // method: uno de los 4 literales, requerido
    let method = match obj.get("method") {
        None => {
            issues.push(required_issue("method"));
            None
        }
        Some(Value::String(s)) => match s.as_str() {
            "offer" => Some(Method::Offer),
            "answer" => Some(Method::Answer),
            "iceCandidate" => Some(Method::IceCandidate),
            "iceCandidates" => Some(Method::IceCandidates),
            _ => {
                issues.push(ValidationIssue {
                    path: vec!["method".to_string()],
                    message:
                        "invalid method, expected one of: offer, answer, iceCandidate, iceCandidates"
                            .to_string(),
                });
                None
            }
        },
        Some(_) => {
            issues.push(type_issue("method", "string"));
            None
        }
    };

    // source: opcional, pero si viene debe ser "wallet" | "extension"
    let source = match obj.get("source") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "wallet" || s == "extension" => Some(s.clone()),
        Some(Value::String(_)) => {
            issues.push(ValidationIssue {
                path: vec!["source".to_string()],
                message: "invalid source, expected one of: wallet, extension".to_string(),
            });
            None
        }
        Some(_) => {
            issues.push(type_issue("source", "string"));
            None
        }
    };

    // connectionId: opcional, string si viene
    let connection_id = match obj.get("connectionId") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            issues.push(type_issue("connectionId", "string"));
            None
        }
    };

    // encryptedPayload: string, requerido
    let encrypted_payload = match obj.get("encryptedPayload") {
        None => {
            issues.push(required_issue("encryptedPayload"));
            None
        }
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            issues.push(type_issue("encryptedPayload", "string"));
            None
        }
    };

    // targetClientId: string, requerido
    let target_client_id = match obj.get("targetClientId") {
        None => {
            issues.push(required_issue("targetClientId"));
            None
        }
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            issues.push(type_issue("targetClientId", "string"));
            None
        }
    };

    // startAt: opcional, number si viene
    let start_at = match obj.get("startAt") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => n.as_f64(),
        Some(_) => {
            issues.push(type_issue("startAt", "number"));
            None
        }
    };

    if !issues.is_empty() {
        return Err(issues);
    }

    // En este punto todos los campos requeridos están presentes (si no,
    // ya habríamos retornado Err arriba), así que los unwrap() son seguros.
    Ok(Message {
        request_id: request_id.unwrap(),
        method: method.unwrap(),
        source,
        connection_id,
        encrypted_payload: encrypted_payload.unwrap(),
        target_client_id: target_client_id.unwrap(),
        start_at,
    })
}

fn required_issue(field: &str) -> ValidationIssue {
    ValidationIssue {
        path: vec![field.to_string()],
        message: "required".to_string(),
    }
}

fn type_issue(field: &str, expected: &str) -> ValidationIssue {
    ValidationIssue {
        path: vec![field.to_string()],
        message: format!("expected {expected}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Equivalente directo de `validate.test.ts`: un objeto vacío debe
    /// producir exactamente 4 errores (requestId, method, encryptedPayload,
    /// targetClientId — los 4 campos requeridos).
    #[test]
    fn empty_object_produces_four_errors() {
        let value: Value = serde_json::json!({});
        let result = validate_message(&value);
        assert!(result.is_err());
        let issues = result.unwrap_err();
        assert_eq!(issues.len(), 4);
    }

    #[test]
    fn valid_offer_message_passes() {
        let value = serde_json::json!({
            "requestId": "abc-123",
            "method": "offer",
            "encryptedPayload": "deadbeef",
            "targetClientId": "some-target-id"
        });
        let result = validate_message(&value);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(msg.request_id, "abc-123");
        assert_eq!(msg.method, Method::Offer);
    }

    #[test]
    fn invalid_method_is_rejected() {
        let value = serde_json::json!({
            "requestId": "abc-123",
            "method": "not-a-real-method",
            "encryptedPayload": "deadbeef",
            "targetClientId": "some-target-id"
        });
        let result = validate_message(&value);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().len(), 1);
    }

    #[test]
    fn redundant_optional_fields_are_accepted() {
        // source y connectionId son campos redundantes heredados, pero
        // el original los acepta si vienen -- no deben producir error.
        let value = serde_json::json!({
            "requestId": "abc-123",
            "method": "answer",
            "source": "wallet",
            "connectionId": "aaaa",
            "encryptedPayload": "deadbeef",
            "targetClientId": "some-target-id",
            "startAt": 12345
        });
        let result = validate_message(&value);
        assert!(result.is_ok());
    }
}
