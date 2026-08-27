//! The classic-mode consume loop: manual acks, bounded prefetch, batched
//! flushes, ack-after-commit.
//!
//! Invariants (architecture doc §5.3-§5.5, CLAUDE.md):
//! - a delivery is acked only after the database commit containing it;
//! - poison input dead-letters, it never wedges or crashes the loop;
//! - there are no test-only branches here: what this loop survives, it
//!   survives for real.

use std::time::Duration;

use coldbore_proto::config::IngestConfig;
use coldbore_proto::metrics::{IngestMetrics, LatencyPercentiles, event_kind};
use coldbore_proto::topology::{
    CONTROL_EXCHANGE, FRAMES_BINDING, FRAMES_DLQ, FRAMES_DLX, FRAMES_EXCHANGE, FRAMES_QUEUE,
    TELEMETRY_EXCHANGE, events_routing_key, metrics_routing_key,
};
use coldbore_proto::{Frame, now_ms};
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, BasicRejectOptions,
    ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use serde_json::json;
use tracing::{info, warn};

use crate::gap::{GapChange, GapTracker};
use crate::sink::Sink;
use crate::{control, telemetry::Counters};

/// One connected consume session. Returns `Err` when the broker or database
/// connection dies; the supervisor in `main` reconnects. `gaps` and
/// `counters` outlive the session on purpose: accounting does not reset just
/// because a connection did.
pub async fn run_classic(
    cfg: &IngestConfig,
    counters: &mut Counters,
    gaps: &mut GapTracker,
) -> anyhow::Result<()> {
    let conn = Connection::connect(
        &cfg.common.amqp_url,
        ConnectionProperties::default().with_connection_name("coldbore-ingest".into()),
    )
    .await?;
    let channel = conn.create_channel().await?;
    declare_topology(&channel).await?;
    channel
        .basic_qos(cfg.prefetch, BasicQosOptions::default())
        .await?;

    let ctrl_channel = conn.create_channel().await?;
    let ctrl = tokio::spawn(control::run(ctrl_channel));

    let sink = Sink::connect(&cfg.pg_dsn).await?;
    // Seed gap accounting from the durable store so a restarted consumer
    // does not report already-landed history as open gaps (spec 005).
    for (pad, well, epoch, max_seq) in sink.watermarks().await? {
        gaps.seed(pad, well, epoch, max_seq);
    }
    let mut consumer = channel
        .basic_consume(
            FRAMES_QUEUE.into(),
            "cb-ingest".into(),
            BasicConsumeOptions::default(), // manual ack
            FieldTable::default(),
        )
        .await?;
    info!(prefetch = cfg.prefetch, "consuming from classic queue");
    let _ = publish_event(
        &channel,
        event_kind::SERVICE_STARTED,
        json!({"mode": "classic", "prefetch": cfg.prefetch}),
    )
    .await;

    // Batch state. Latency histogram covers frames committed since the last
    // metrics tick; 1 ms .. 1 h at 3 significant figures.
    let mut batch: Vec<Frame> = Vec::with_capacity(cfg.batch_max_frames);
    let mut highest_tag: u64 = 0;
    let mut hist = Histogram::<u64>::new_with_bounds(1, 3_600_000, 3)?;
    let mut flush_tick = tokio::time::interval(Duration::from_millis(cfg.batch_max_ms));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut metrics_tick =
        tokio::time::interval(Duration::from_millis(cfg.common.metrics_interval_ms));
    metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let result = loop {
        tokio::select! {
            delivery = consumer.next() => {
                match delivery {
                    Some(Ok(d)) => {
                        accept(cfg, counters, &mut batch, &mut highest_tag, d).await;
                        if batch.len() >= cfg.batch_max_frames
                            && let Err(e) = flush(&sink, &channel, counters, gaps, &mut batch, &mut highest_tag, &mut hist).await
                        {
                            break Err(e);
                        }
                    }
                    Some(Err(e)) => break Err(e.into()),
                    None => break Err(anyhow::anyhow!("consumer stream ended (cancelled?)")),
                }
            }
            _ = flush_tick.tick() => {
                if !batch.is_empty()
                    && let Err(e) = flush(&sink, &channel, counters, gaps, &mut batch, &mut highest_tag, &mut hist).await
                {
                    break Err(e);
                }
            }
            _ = metrics_tick.tick() => {
                publish_metrics(&channel, cfg, counters, gaps, &mut hist).await;
            }
        }
    };
    ctrl.abort();
    result
}

/// Parse-or-dead-letter. Poison never reaches the batch.
async fn accept(
    _cfg: &IngestConfig,
    counters: &mut Counters,
    batch: &mut Vec<Frame>,
    highest_tag: &mut u64,
    delivery: Delivery,
) {
    counters.consumed += 1;
    if delivery.redelivered {
        counters.redeliveries += 1;
    }
    let parsed = serde_json::from_slice::<Frame>(&delivery.data)
        .map_err(|e| e.to_string())
        .and_then(|f| f.validate().map(|_| f).map_err(|e| e.to_string()));
    match parsed {
        Ok(frame) => {
            *highest_tag = delivery.delivery_tag;
            batch.push(frame);
        }
        Err(reason) => {
            counters.poison += 1;
            warn!(reason, "poison frame -> dead letter");
            // requeue=false + queue-level DLX routes it to cb.frames.dlq.
            if let Err(e) = delivery.reject(BasicRejectOptions { requeue: false }).await {
                warn!(error = %e, "reject failed");
            }
        }
    }
}

/// Commit, account, then ack: strictly in that order.
async fn flush(
    sink: &Sink,
    channel: &Channel,
    counters: &mut Counters,
    gaps: &mut GapTracker,
    batch: &mut Vec<Frame>,
    highest_tag: &mut u64,
    hist: &mut Histogram<u64>,
) -> anyhow::Result<()> {
    let inserted = sink.insert_batch(batch).await?;
    let now = now_ms();
    counters.inserted += inserted;
    counters.dup_dropped += batch.len() as u64 - inserted;
    counters.batches += 1;

    for frame in batch.iter() {
        hist.saturating_record(now.saturating_sub(frame.t_ms).max(1));
        for change in gaps.observe(frame.pad, frame.well, frame.epoch, frame.seq, now) {
            publish_gap_event(channel, &change).await;
        }
    }

    // Multiple-ack everything up to the newest delivery in this batch.
    // Poison deliveries below the tag were already settled by reject.
    channel
        .basic_ack(*highest_tag, BasicAckOptions { multiple: true })
        .await?;
    batch.clear();
    Ok(())
}

async fn publish_metrics(
    channel: &Channel,
    cfg: &IngestConfig,
    counters: &Counters,
    gaps: &GapTracker,
    hist: &mut Histogram<u64>,
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
        mode: cfg.common.mode.as_str().to_string(),
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
        committed_offset: None,
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

async fn publish_gap_event(channel: &Channel, change: &GapChange) {
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

async fn publish_event(
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

/// Idempotent topology declaration: exchanges, the DLX pair, and the frames
/// queue wired to dead-letter into it.
async fn declare_topology(channel: &Channel) -> lapin::Result<()> {
    let durable_exchange = ExchangeDeclareOptions {
        durable: true,
        ..ExchangeDeclareOptions::default()
    };
    channel
        .exchange_declare(
            FRAMES_EXCHANGE.into(),
            ExchangeKind::Topic,
            durable_exchange,
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            CONTROL_EXCHANGE.into(),
            ExchangeKind::Fanout,
            durable_exchange,
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            TELEMETRY_EXCHANGE.into(),
            ExchangeKind::Topic,
            durable_exchange,
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            FRAMES_DLX.into(),
            ExchangeKind::Fanout,
            durable_exchange,
            FieldTable::default(),
        )
        .await?;

    let durable_queue = QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    };
    channel
        .queue_declare(FRAMES_DLQ.into(), durable_queue, FieldTable::default())
        .await?;
    channel
        .queue_bind(
            FRAMES_DLQ.into(),
            FRAMES_DLX.into(),
            "".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut args = FieldTable::default();
    args.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString(FRAMES_DLX.into()),
    );
    channel
        .queue_declare(FRAMES_QUEUE.into(), durable_queue, args)
        .await?;
    channel
        .queue_bind(
            FRAMES_QUEUE.into(),
            FRAMES_EXCHANGE.into(),
            FRAMES_BINDING.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}
