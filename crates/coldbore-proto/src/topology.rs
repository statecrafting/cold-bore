//! Broker object names (architecture doc §3). Every service declares the
//! objects it uses idempotently at startup; identical names and durability
//! flags everywhere are what make that safe.

/// Topic exchange the edge publishes frames to.
pub const FRAMES_EXCHANGE: &str = "cb.frames.x";
/// Classic durable queue (classic mode data path), bound `frames.#`.
pub const FRAMES_QUEUE: &str = "cb.frames.q";
/// Binding pattern from frames queue to frames exchange.
pub const FRAMES_BINDING: &str = "frames.#";
/// Dead-letter exchange + queue for poison frames.
pub const FRAMES_DLX: &str = "cb.frames.dlx";
pub const FRAMES_DLQ: &str = "cb.frames.dlq";
/// Stream (stream mode data path, phase 3).
pub const FRAMES_STREAM: &str = "cb.frames.s";

/// Fanout exchange for control commands.
pub const CONTROL_EXCHANGE: &str = "cb.control.x";

/// Topic exchange for metric snapshots and events.
pub const TELEMETRY_EXCHANGE: &str = "cb.telemetry.x";
/// The api's binding queue on the telemetry exchange.
pub const TELEMETRY_API_QUEUE: &str = "cb.telemetry.api.q";

pub fn metrics_routing_key(service: &str) -> String {
    format!("metrics.{service}")
}

pub fn events_routing_key(kind: &str) -> String {
    format!("events.{kind}")
}
