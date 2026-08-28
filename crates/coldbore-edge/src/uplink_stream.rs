//! The native stream-protocol publisher (spec 008): the throughput upgrade
//! for the confirm-path ceiling found in benchmark 001.
//!
//! Custody is the same machine as classic mode (`UplinkState`): a frame
//! leaves edge custody only on a positive confirmation. What changes:
//!
//! - publishes go straight to the stream over the stream protocol (port
//!   5552) in batches, not through the exchange one message at a time;
//! - the producer is a **named dedup producer** (`cb-edge-{epoch}`): every
//!   message carries a monotonically increasing publishing id, and a
//!   retransmission reuses its original id, so the broker itself drops
//!   confirm-loss duplicates. Injected duplicates (the fault) get fresh ids
//!   on purpose: they must reach the sink to demonstrate its absorption.
//!   The name embeds the epoch so a restarted producer starts a fresh dedup
//!   timeline instead of being silently deduplicated against its previous
//!   life's ids.
//! - the AMQP connection remains for control, telemetry, and topology.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use anyhow::Context;
use coldbore_proto::Frame;
use coldbore_proto::config::EdgeConfig;
use coldbore_proto::metrics::event_kind;
use coldbore_proto::topology::FRAMES_STREAM;
use lapin::{Connection, ConnectionProperties};
use rabbitmq_stream_client::types::Message;
use rabbitmq_stream_client::{Dedup, Environment, Producer};
use rand::RngExt;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{info, warn};

use crate::faults::FaultBox;
use crate::telemetry::{self, Counters};
use crate::uplink::{INITIAL_BACKOFF, MAX_BACKOFF, UplinkState};
use crate::{control, uplink};

/// Frames sent per `batch_send` call.
const PUBLISH_BATCH: usize = 128;
/// A confirmation older than this is presumed lost; the frame retransmits
/// (same publishing id, so the broker dedups if it did arrive).
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);
/// Frames in flight but zero confirms arriving for this long: the stream
/// connection is presumed dead (retransmit sweeps alone would cycle
/// forever); tear the session down and rebuild both connections.
const CONFIRM_STALL_TIMEOUT: Duration = Duration::from_secs(30);
/// A `batch_send` that does not return within this bound means a hung
/// socket, not a slow broker.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Stream-specific custody: survives reconnects alongside `UplinkState`.
struct StreamCustody {
    /// Next publishing id (monotonic per producer name = per process).
    next_id: u64,
    /// Retransmits that must reuse their original publishing id.
    retry_ids: Vec<(Frame, u64)>,
}

pub async fn run(
    cfg: EdgeConfig,
    faults: FaultBox,
    counters: Arc<Counters>,
    mut rx: Receiver<Frame>,
    epoch: u64,
) {
    let mut st = UplinkState::new(&cfg);
    let mut custody = StreamCustody {
        next_id: 0,
        retry_ids: Vec::new(),
    };
    let producer_name = format!("cb-edge-{epoch}");
    loop {
        match connect_and_serve(
            &cfg,
            &faults,
            &counters,
            &mut rx,
            &mut st,
            &mut custody,
            &producer_name,
        )
        .await
        {
            Ok(()) => return,
            Err(e) => {
                warn!(error = %e, backoff_ms = st.backoff.as_millis() as u64, "stream uplink lost")
            }
        }
        let deadline = Instant::now() + st.backoff;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(frame)) => st.buffer_frame(frame, &counters),
                Ok(None) => return,
                Err(_) => break,
            }
        }
        counters.buffered.store(st.buffered(), Relaxed);
        st.backoff = (st.backoff * 2).min(MAX_BACKOFF);
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_and_serve(
    cfg: &EdgeConfig,
    faults: &FaultBox,
    counters: &Arc<Counters>,
    rx: &mut Receiver<Frame>,
    st: &mut UplinkState,
    custody: &mut StreamCustody,
    producer_name: &str,
) -> anyhow::Result<()> {
    // AMQP side: topology (exchanges + queues + the stream), control, metrics.
    let conn = Connection::connect(
        &cfg.common.amqp_url,
        ConnectionProperties::default().with_connection_name("coldbore-edge".into()),
    )
    .await?;
    let channel = conn.create_channel().await?;
    uplink::declare_topology(&channel).await?;
    let ctrl_channel = conn.create_channel().await?;
    let ctrl = tokio::spawn(control::run(ctrl_channel, faults.clone()));
    let met_channel = conn.create_channel().await?;
    let met = tokio::spawn(telemetry::metrics_task(
        met_channel,
        counters.clone(),
        faults.clone(),
        cfg.clone(),
    ));

    let result = async {
        let environment = Environment::builder()
            .host(&cfg.common.stream_host)
            .port(cfg.common.stream_port)
            .username(&cfg.common.stream_user)
            .password(&cfg.common.stream_pass)
            .build()
            .await
            .context("stream environment")?;
        let producer = environment
            .producer()
            .name(producer_name)
            .batch_size(PUBLISH_BATCH)
            .build(FRAMES_STREAM)
            .await
            .map_err(|e| anyhow::anyhow!("stream producer build: {e}"))?;
        info!(producer = producer_name, "stream uplink connected");
        st.backoff = INITIAL_BACKOFF;
        let _ = telemetry::publish_event(
            &channel,
            event_kind::SERVICE_STARTED,
            "edge",
            serde_json::json!({"mode": "stream", "buffered": st.buffered()}),
        )
        .await;
        let mut producer = producer;
        let outcome = publish_loop(
            cfg,
            faults,
            counters,
            rx,
            st,
            custody,
            &mut producer,
            &channel,
        )
        .await;
        let _ = producer.close().await;
        outcome
    }
    .await;

    ctrl.abort();
    met.abort();
    result
}

#[allow(clippy::too_many_arguments)]
async fn publish_loop(
    cfg: &EdgeConfig,
    faults: &FaultBox,
    counters: &Arc<Counters>,
    rx: &mut Receiver<Frame>,
    st: &mut UplinkState,
    custody: &mut StreamCustody,
    producer: &mut Producer<Dedup>,
    channel: &lapin::Channel,
) -> anyhow::Result<()> {
    let (conf_tx, mut conf_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, bool)>();
    // publishing id -> (frame, is_dup, sent_at)
    let mut inflight: HashMap<u64, (Frame, bool, Instant)> = HashMap::new();
    let mut drain_tick = tokio::time::interval(Duration::from_millis(20));
    drain_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sweep_tick = tokio::time::interval(Duration::from_secs(1));
    sweep_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_confirm = Instant::now();

    loop {
        // Pump: batch publishes up to the confirm window.
        while inflight.len() < cfg.confirm_window {
            let room = (cfg.confirm_window - inflight.len()).min(PUBLISH_BATCH);
            let mut messages = Vec::with_capacity(room);
            let mut staged: Vec<u64> = Vec::with_capacity(room);
            // Retransmits first, reusing their original publishing ids.
            while messages.len() < room {
                if let Some((frame, id)) = custody.retry_ids.pop() {
                    messages.push(build_message(&frame, id));
                    inflight.insert(id, (frame, false, Instant::now()));
                    staged.push(id);
                } else {
                    break;
                }
            }
            while messages.len() < room {
                let Some((frame, is_dup)) = st.next_publishable(faults) else {
                    break;
                };
                let id = custody.next_id;
                custody.next_id += 1;
                messages.push(build_message(&frame, id));
                inflight.insert(id, (frame, is_dup, Instant::now()));
                staged.push(id);
            }
            if messages.is_empty() {
                break;
            }
            let sent = messages.len() as u64;
            let tx = conf_tx.clone();
            let send = producer.batch_send(messages, move |confirmation| {
                let tx = tx.clone();
                async move {
                    // The Err variant does not identify the message;
                    // the timeout sweep recovers those.
                    if let Ok(status) = confirmation {
                        let _ = tx.send((status.publishing_id(), status.confirmed()));
                    }
                }
            });
            let send_result = match tokio::time::timeout(SEND_TIMEOUT, send).await {
                Ok(r) => r.map_err(|e| anyhow::anyhow!("stream publish failed: {e}")),
                Err(_) => Err(anyhow::anyhow!(
                    "stream publish timed out after {SEND_TIMEOUT:?}; connection presumed dead"
                )),
            };
            if let Err(e) = send_result {
                // Publish failed wholesale: recover the staged frames and
                // reconnect. Same ids on retransmit; the broker dedups any
                // that actually arrived.
                for id in staged {
                    if let Some((frame, is_dup, _)) = inflight.remove(&id)
                        && !is_dup
                    {
                        custody.retry_ids.push((frame, id));
                    }
                }
                drain_inflight_to_retry(&mut inflight, custody);
                return Err(e);
            }
            counters.published.fetch_add(sent, Relaxed);
        }
        counters
            .buffered
            .store(st.buffered() + custody.retry_ids.len() as u64, Relaxed);

        tokio::select! {
            biased;
            confirmed = conf_rx.recv() => {
                // The channel cannot close while conf_tx lives in this scope.
                let Some((id, ok)) = confirmed else { continue };
                last_confirm = Instant::now();
                if let Some((frame, is_dup, _)) = inflight.remove(&id) {
                    if ok {
                        counters.confirmed.fetch_add(1, Relaxed);
                        if !is_dup {
                            let p = faults.dup_rate();
                            if p > 0.0 && rand::rng().random::<f64>() < p {
                                counters.dup_injected.fetch_add(1, Relaxed);
                                st.dup_queue.push_back(frame);
                            }
                        }
                    } else {
                        counters.retransmits.fetch_add(1, Relaxed);
                        if !is_dup {
                            custody.retry_ids.push((frame, id));
                        }
                    }
                }
            }
            received = rx.recv() => {
                match received {
                    Some(frame) => st.buffer_frame(frame, counters),
                    None => {
                        drain_inflight_to_retry(&mut inflight, custody);
                        return Ok(());
                    }
                }
            }
            _ = sweep_tick.tick() => {
                let now = Instant::now();
                if inflight.is_empty() && custody.retry_ids.is_empty() {
                    last_confirm = now;
                } else if now.duration_since(last_confirm) > CONFIRM_STALL_TIMEOUT {
                    let stalled = inflight.len() + custody.retry_ids.len();
                    drain_inflight_to_retry(&mut inflight, custody);
                    return Err(anyhow::anyhow!(
                        "no stream confirm progress for {CONFIRM_STALL_TIMEOUT:?} with {stalled} pending; connection presumed dead"
                    ));
                }
                // AMQP-side liveness (control + telemetry ride it): a dead
                // connection there must also end the session.
                if let Err(e) = uplink::probe_liveness(channel).await {
                    drain_inflight_to_retry(&mut inflight, custody);
                    return Err(e);
                }
                // Confirmations presumed lost: retransmit under the same id.
                let stale: Vec<u64> = inflight
                    .iter()
                    .filter(|(_, (_, _, sent))| now.duration_since(*sent) > CONFIRM_TIMEOUT)
                    .map(|(&id, _)| id)
                    .collect();
                for id in stale {
                    if let Some((frame, is_dup, _)) = inflight.remove(&id) {
                        counters.retransmits.fetch_add(1, Relaxed);
                        if !is_dup {
                            custody.retry_ids.push((frame, id));
                        }
                    }
                }
            }
            _ = drain_tick.tick() => {}
        }
    }
}

fn drain_inflight_to_retry(
    inflight: &mut HashMap<u64, (Frame, bool, Instant)>,
    custody: &mut StreamCustody,
) {
    for (id, (frame, is_dup, _)) in inflight.drain() {
        if !is_dup {
            custody.retry_ids.push((frame, id));
        }
    }
}

fn build_message(frame: &Frame, publishing_id: u64) -> Message {
    Message::builder()
        .body(serde_json::to_vec(frame).unwrap_or_default())
        .publishing_id(publishing_id)
        .build()
}
