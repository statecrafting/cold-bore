//! Telemetry-plane publishing: cumulative counters, the 1 Hz snapshot task,
//! and the event helper. Lost telemetry during a broker outage is accepted
//! (the counters are cumulative, so the next snapshot tells the whole story).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use coldbore_proto::config::EdgeConfig;
use coldbore_proto::metrics::{EdgeMetrics, Event};
use coldbore_proto::now_ms;
use coldbore_proto::topology::{TELEMETRY_EXCHANGE, events_routing_key, metrics_routing_key};
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Channel};
use tracing::warn;

use crate::faults::FaultBox;

#[derive(Default)]
pub struct Counters {
    pub generated: AtomicU64,
    pub published: AtomicU64,
    pub confirmed: AtomicU64,
    pub retransmits: AtomicU64,
    /// Gauge: frames currently in uplink custody (store-and-forward).
    pub buffered: AtomicU64,
    pub buffer_dropped: AtomicU64,
    pub dup_injected: AtomicU64,
}

pub async fn publish_json(
    channel: &Channel,
    routing_key: String,
    body: Vec<u8>,
) -> lapin::Result<()> {
    channel
        .basic_publish(
            TELEMETRY_EXCHANGE.into(),
            routing_key.into(),
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default().with_content_type("application/json".into()),
        )
        .await
        .map(|_confirm| ())
}

pub async fn publish_event(
    channel: &Channel,
    kind: &str,
    service: &str,
    payload: serde_json::Value,
) -> lapin::Result<()> {
    let event = Event {
        kind: kind.to_string(),
        service: service.to_string(),
        t_ms: now_ms(),
        payload,
    };
    let body = serde_json::to_vec(&event).unwrap_or_default();
    publish_json(channel, events_routing_key(kind), body).await
}

pub async fn metrics_task(
    channel: Channel,
    counters: Arc<Counters>,
    faults: FaultBox,
    cfg: EdgeConfig,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(cfg.common.metrics_interval_ms));
    loop {
        tick.tick().await;
        let f = faults.snapshot();
        let snapshot = EdgeMetrics {
            service: "edge".to_string(),
            t_ms: now_ms(),
            generated: counters.generated.load(Relaxed),
            published: counters.published.load(Relaxed),
            confirmed: counters.confirmed.load(Relaxed),
            retransmits: counters.retransmits.load(Relaxed),
            buffered: counters.buffered.load(Relaxed),
            buffer_dropped: counters.buffer_dropped.load(Relaxed),
            dup_injected: counters.dup_injected.load(Relaxed),
            rate_hz: cfg.rate_hz * f.rate_multiplier,
            pads: f.pads,
            wells_per_pad: f.wells_per_pad,
            links: f.links,
        };
        let body = serde_json::to_vec(&snapshot).unwrap_or_default();
        // Bounded: on a half-dead connection a publish can hang instead of
        // erroring; either way the task exits and reconnect respawns it.
        match tokio::time::timeout(
            Duration::from_secs(5),
            publish_json(&channel, metrics_routing_key("edge"), body),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(error = %e, "metrics publish failed; task exiting until reconnect");
                return;
            }
            Err(_) => {
                warn!("metrics publish timed out; task exiting until reconnect");
                return;
            }
        }
    }
}
