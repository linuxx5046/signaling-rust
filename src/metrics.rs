use prometheus::process_collector::ProcessCollector;
use prometheus::{Counter, Gauge, Registry};

/// Métricas custom del servidor (sección 6 de la especificación) +
/// ProcessCollector para las métricas de proceso (sección 6.1).
pub struct Metrics {
    pub registry: Registry,
    pub connected_clients: Gauge,
    pub incoming_messages: Counter,
    pub outgoing_messages: Counter,
    pub publish_messages: Counter,
    // [IGUAL QUE ORIGINAL -- decisión 9.3] se registra y se expone siempre
    // en 0 pero nunca se incrementa, igual que `subscribeMessageCounter`
    // en el original. El warning de dead_code es esperado y correcto.
    #[allow(dead_code)]
    pub subscribe_messages: Counter,
    pub redis_get_time: Gauge,
    pub redis_set_time: Gauge,
    pub redis_delete_time: Gauge,
    pub redis_publish_time: Gauge,
    pub redis_subscribe_time: Gauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        macro_rules! register_counter {
            ($name:expr, $help:expr) => {{
                let c = Counter::new($name, $help).unwrap();
                registry.register(Box::new(c.clone())).unwrap();
                c
            }};
        }
        macro_rules! register_gauge {
            ($name:expr, $help:expr) => {{
                let g = Gauge::new($name, $help).unwrap();
                registry.register(Box::new(g.clone())).unwrap();
                g
            }};
        }

        let connected_clients = register_gauge!(
            "signaling_server_connected_clients",
            "The number of connected clients"
        );
        let incoming_messages = register_counter!(
            "signaling_server_incoming_messages",
            "Number of incoming messages"
        );
        let outgoing_messages = register_counter!(
            "signaling_server_outgoing_messages",
            "Number of outgoing messages"
        );
        let publish_messages = register_counter!(
            "signaling_server_publish_messages",
            "Number of published messages"
        );
        let subscribe_messages = register_counter!(
            "signaling_server_subscribe_messages",
            "Number of subscribed messages"
        );
        let redis_get_time = register_gauge!(
            "signaling_server_redis_get_time",
            "The time it takes in milliseconds for redis to get values from set"
        );
        let redis_set_time = register_gauge!(
            "signaling_server_redis_set_time",
            "The time it takes in milliseconds for redis to add value to set"
        );
        let redis_delete_time = register_gauge!(
            "signaling_server_redis_delete_time",
            "The time it takes in milliseconds for redis to delete value from set"
        );
        let redis_publish_time = register_gauge!(
            "signaling_server_redis_publish_time",
            "The time it takes for redis to publish to data channel"
        );
        let redis_subscribe_time = register_gauge!(
            "signaling_server_redis_subscribe_time",
            "The time it takes for redis to subscribe to data channel"
        );

        // Métricas de proceso estándar de Prometheus (process_resident_memory_bytes,
        // process_cpu_seconds_total, process_open_fds, etc. — sección 6.1 de la spec).
        // Solo funciona en Linux, que es la plataforma de despliegue real.
        let process_collector = ProcessCollector::for_self();
        registry
            .register(Box::new(process_collector))
            .expect("failed to register process collector");

        Metrics {
            registry,
            connected_clients,
            incoming_messages,
            outgoing_messages,
            publish_messages,
            subscribe_messages,
            redis_get_time,
            redis_set_time,
            redis_delete_time,
            redis_publish_time,
            redis_subscribe_time,
        }
    }

    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}
