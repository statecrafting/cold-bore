//! Ingest accounting and telemetry-plane publishing, shared by the classic
//! and stream consume loops. The loops are single-threaded, so plain fields
//! suffice for counters.

use coldbore_proto::metrics::{IngestMetrics, LatencyPercentiles};
use coldbore_proto::now_ms;
use coldbore_proto::topology::{TELEMETRY_EXCHANGE, events_routing_key, metrics_routing_key};
use hdrhistogram::Histogram;
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Channel};
use serde_json::json;
use tracing::warn;

use crate::gap::{GapChange, GapTracker};

#[derive(Debug, Default)]
pub struct Counters {
    pub consumed: u64,
    pub inserted: u64,
    pub dup_dropped: u64,
    pub poison: u64,
    pub redeliveries: u64,
    pub batches: u64,
}

pub async fn publish_event(
    channel: &Channel,
    kind: &str,
    payload: serde_json::Value,
) -> lapin::Result<()> {
    let event = coldbore_proto::Event {
        kind: kind.to_string(),
        service: "ingest".to_string(),
        t_ms: now_ms(),
        payload,
    };
    let body = serde_json::to_vec(&event).unwrap_or_default();
    channel
        .basic_publish(
            TELEMETRY_EXCHANGE.into(),
            events_routing_key(kind).into(),
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default().with_content_type("application/json".into()),
        )
        .await
        .map(|_| ())
}

pub async fn publish_gap_event(channel: &Channel, change: &GapChange) {
    use coldbore_proto::metrics::event_kind;
    let (kind, payload) = match *change {
        GapChange::Opened {
            pad,
            well,
            from,
            to,
        } => (
            event_kind::GAP_OPENED,
            json!({"pad": pad, "well": well, "from": from, "to": to, "span": to - from + 1}),
        ),
        GapChange::Healed {
            pad,
            well,
            from,
            to,
            after_ms,
        } => (
            event_kind::GAP_HEALED,
            json!({"pad": pad, "well": well, "from": from, "to": to, "span": to - from + 1, "after_ms": after_ms}),
        ),
    };
    let _ = publish_event(channel, kind, payload).await;
}

/// Drain the latency histogram into a 1 Hz snapshot and publish it.
#[allow(clippy::too_many_arguments)]
pub async fn publish_metrics(
    channel: &Channel,
    mode: &str,
    counters: &Counters,
    gaps: &GapTracker,
    hist: &mut Histogram<u64>,
    committed_offset: Option<u64>,
) {
    let e2e = if !hist.is_empty() {
        Some(LatencyPercentiles {
            p50_ms: hist.value_at_quantile(0.50) as f64,
            p95_ms: hist.value_at_quantile(0.95) as f64,
            p99_ms: hist.value_at_quantile(0.99) as f64,
            max_ms: hist.max() as f64,
        })
    } else {
        None
    };
    hist.reset();
    let snapshot = IngestMetrics {
        service: "ingest".to_string(),
        t_ms: now_ms(),
        mode: mode.to_string(),
        consumed: counters.consumed,
        inserted: counters.inserted,
        dup_dropped: counters.dup_dropped,
        poison: counters.poison,
        redeliveries: counters.redeliveries,
        batches: counters.batches,
        open_gaps: gaps.open_now(),
        gaps_opened: gaps.opened(),
        gaps_healed: gaps.healed(),
        e2e,
        committed_offset,
    };
    let body = serde_json::to_vec(&snapshot).unwrap_or_default();
    if let Err(e) = channel
        .basic_publish(
            TELEMETRY_EXCHANGE.into(),
            metrics_routing_key("ingest").into(),
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default().with_content_type("application/json".into()),
        )
        .await
    {
        warn!(error = %e, "metrics publish failed");
    }
}
