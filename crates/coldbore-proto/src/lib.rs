//! Shared vocabulary for the cold-bore pipeline.
//!
//! Spec: 002-telemetry-model. The JSON shapes defined here are the
//! cross-language contract with `services/api` (Python); changing them means
//! changing the owning spec, both implementations, and
//! `docs/design/architecture.md` in the same PR.

pub mod config;
pub mod control;
pub mod frame;
pub mod metrics;
pub mod topology;

pub use control::{ControlCommand, LinkState, ServiceId};
pub use frame::Frame;
pub use metrics::{EdgeMetrics, Event, IngestMetrics, LatencyPercentiles};

/// Current wall-clock time in milliseconds since the Unix epoch.
///
/// Event time (`Frame::t_ms`) and metric timestamps both use this scale.
pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
