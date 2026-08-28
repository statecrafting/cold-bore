//! Telemetry-plane contracts: 1 Hz metric snapshots and discrete events.
//!
//! Counters are cumulative (monotonic since process start); consumers derive
//! rates by differencing successive snapshots. That keeps producers stateless
//! about who is watching and makes missed snapshots harmless.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeMetrics {
    /// Always `"edge"`; snapshots self-describe so a mixed stream stays legible.
    pub service: String,
    pub t_ms: u64,
    pub generated: u64,
    pub published: u64,
    pub confirmed: u64,
    pub retransmits: u64,
    /// Frames currently held by store-and-forward across all pads.
    pub buffered: u64,
    /// Frames dropped from full store-and-forward buffers (drop-oldest).
    pub buffer_dropped: u64,
    pub dup_injected: u64,
    /// Effective generation frequency per well after the rate multiplier.
    pub rate_hz: f64,
    /// Current field size (runtime-adjustable via the `topology` command).
    pub pads: u16,
    pub wells_per_pad: u16,
    /// Pad id -> link up?
    pub links: BTreeMap<u16, bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestMetrics {
    /// Always `"ingest"`.
    pub service: String,
    pub t_ms: u64,
    /// `"classic"` or `"stream"`.
    pub mode: String,
    pub consumed: u64,
    pub inserted: u64,
    /// Idempotent-sink conflicts: the observable measure of duplicate absorption.
    pub dup_dropped: u64,
    pub poison: u64,
    /// Deliveries with the broker's redelivered flag set.
    pub redeliveries: u64,
    pub batches: u64,
    pub open_gaps: u64,
    pub gaps_opened: u64,
    pub gaps_healed: u64,
    /// End-to-end latency over frames committed in the last second;
    /// absent when nothing was flushed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub e2e: Option<LatencyPercentiles>,
    /// Stream mode only: last offset stored after a commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_offset: Option<u64>,
}

/// Discrete happenings, persisted by the api into the `events` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub kind: String,
    pub service: String,
    pub t_ms: u64,
    pub payload: serde_json::Value,
}

/// Event kind vocabulary. Free-form kinds are allowed; these are the ones the
/// dashboard and scoring know by name.
pub mod event_kind {
    pub const GAP_OPENED: &str = "gap_opened";
    pub const GAP_HEALED: &str = "gap_healed";
    pub const BUFFER_OVERFLOW: &str = "buffer_overflow";
    pub const FAULT_APPLIED: &str = "fault_applied";
    pub const SERVICE_STARTED: &str = "service_started";
    pub const SERVICE_STOPPING: &str = "service_stopping";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_snapshot_omits_empty_optionals() {
        let m = IngestMetrics {
            service: "ingest".into(),
            t_ms: 1,
            mode: "classic".into(),
            consumed: 10,
            inserted: 9,
            dup_dropped: 1,
            poison: 0,
            redeliveries: 0,
            batches: 1,
            open_gaps: 0,
            gaps_opened: 0,
            gaps_healed: 0,
            e2e: None,
            committed_offset: None,
        };
        let json = serde_json::to_string(&m).expect("serialize");
        assert!(!json.contains("e2e"));
        assert!(!json.contains("committed_offset"));
    }
}
