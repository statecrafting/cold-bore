//! The stream-mode consume loop (spec 008): the native stream protocol,
//! offset-tracked, batched into the same idempotent sink.
//!
//! What changes vs classic (consume.rs), and what deliberately does not:
//! - "what did we commit" moves from the broker's unacked ledger to an
//!   explicit offset. The offset is stored **in the same database
//!   transaction as the batch it covers** (`stream_offsets`), so a restart
//!   resumes exactly where the data ends; the broker-side offset store is
//!   updated after commit as best-effort observability.
//! - reads are non-destructive: replay from any offset is a flag
//!   (`CB_STREAM_FROCE_FROM` + `CB_STREAM_FROM`), and there is no DLQ to
//!   route poison into: poison is counted and skipped, not destroyed.
//! - the AMQP connection remains for control, telemetry, and topology: the
//!   data plane is the only thing that migrated.
//! - ack-after-commit becomes store-offset-after-commit; the sink and gap
//!   accounting are unchanged. The guarantees live in the data model, not
//!   the transport: that is the point of the exercise.

use std::time::Duration;

use anyhow::Context;
use coldbore_proto::config::{IngestConfig, StreamFrom};
use coldbore_proto::metrics::event_kind;
use coldbore_proto::topology::{FRAMES_STREAM, STREAM_CONSUMER_NAME};
use coldbore_proto::{Frame, now_ms};
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use lapin::{Connection, ConnectionProperties};
use rabbitmq_stream_client::types::OffsetSpecification;
use rabbitmq_stream_client::{Consumer, Environment};
use serde_json::json;
use tracing::{info, warn};

use crate::gap::GapTracker;
use crate::sink::Sink;
use crate::telemetry::{self, Counters};
use crate::{consume, control};

/// One connected stream session; the supervisor in `main` reconnects on
/// error, and the transactional offset makes the resume exact.
pub async fn run_stream(
    cfg: &IngestConfig,
    counters: &mut Counters,
    gaps: &mut GapTracker,
) -> anyhow::Result<()> {
    // AMQP side: topology (including the stream itself), control, telemetry.
    let conn = Connection::connect(
        &cfg.common.amqp_url,
        ConnectionProperties::default().with_connection_name("coldbore-ingest".into()),
    )
    .await?;
    let channel = conn.create_channel().await?;
    consume::declare_topology(&channel).await?;
    let ctrl_channel = conn.create_channel().await?;
    let ctrl = tokio::spawn(control::run(ctrl_channel));

    let mut sink = Sink::connect(&cfg.pg_dsn).await?;
    for (pad, well, epoch, max_seq) in sink.watermarks().await? {
        gaps.seed(pad, well, epoch, max_seq);
    }

    // Start position: the transactional offset wins unless a replay is
    // forced; without either, CB_STREAM_FROM decides (default: First, a
    // full re-materialization, because the stream retains history).
    let stored = sink.stored_offset(STREAM_CONSUMER_NAME).await?;
    let (start, start_desc) = match (cfg.stream_force_from, stored) {
        (false, Some(offset)) => (
            OffsetSpecification::Offset(offset + 1),
            format!("resume (stored offset {offset} + 1)"),
        ),
        _ => match cfg.stream_from {
            StreamFrom::First => (OffsetSpecification::First, "first".to_string()),
            StreamFrom::Next => (OffsetSpecification::Next, "next".to_string()),
            StreamFrom::Offset(n) => (OffsetSpecification::Offset(n), format!("offset {n}")),
        },
    };

    let environment = Environment::builder()
        .host(&cfg.common.stream_host)
        .port(cfg.common.stream_port)
        .username(&cfg.common.stream_user)
        .password(&cfg.common.stream_pass)
        .build()
        .await
        .context("stream environment")?;
    let mut consumer = environment
        .consumer()
        .name(STREAM_CONSUMER_NAME)
        .offset(start)
        .build(FRAMES_STREAM)
        .await
        .map_err(|e| anyhow::anyhow!("stream consumer build: {e}"))?;

    info!(start = %start_desc, "consuming from stream");
    let _ = telemetry::publish_event(
        &channel,
        event_kind::SERVICE_STARTED,
        json!({"mode": "stream", "start": start_desc}),
    )
    .await;

    let mut batch: Vec<Frame> = Vec::with_capacity(cfg.batch_max_frames);
    let mut last_offset: u64 = 0;
    let mut committed: Option<u64> = stored;
    let mut hist = Histogram::<u64>::new_with_bounds(1, 3_600_000, 3)?;
    let mut flush_tick = tokio::time::interval(Duration::from_millis(cfg.batch_max_ms));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut metrics_tick =
        tokio::time::interval(Duration::from_millis(cfg.common.metrics_interval_ms));
    metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The probe rides the AMQP side. In the post-sleep failure both
    // connections die together, so breaking the session (and rebuilding the
    // stream consumer with it) covers the data plane too.
    let mut probe_tick = tokio::time::interval(consume::PROBE_INTERVAL);
    probe_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let result = loop {
        tokio::select! {
            delivery = consumer.next() => {
                match delivery {
                    Some(Ok(d)) => {
                        counters.consumed += 1;
                        last_offset = d.offset();
                        let parsed = d
                            .message()
                            .data()
                            .ok_or_else(|| "empty message body".to_string())
                            .and_then(|bytes| {
                                serde_json::from_slice::<Frame>(bytes).map_err(|e| e.to_string())
                            })
                            .and_then(|f| f.validate().map(|_| f).map_err(|e| e.to_string()));
                        match parsed {
                            Ok(frame) => batch.push(frame),
                            Err(reason) => {
                                // No DLQ in a stream: reads are
                                // non-destructive, so poison is counted and
                                // skipped, never destroyed or requeued.
                                counters.poison += 1;
                                warn!(reason, offset = last_offset, "poison record skipped");
                            }
                        }
                        if batch.len() >= cfg.batch_max_frames
                            && let Err(e) = flush(&mut sink, &channel, counters, gaps, &mut batch, last_offset, &mut committed, &consumer, &mut hist).await
                        {
                            break Err(e);
                        }
                    }
                    Some(Err(e)) => break Err(e.into()),
                    None => break Err(anyhow::anyhow!("stream consumer ended")),
                }
            }
            _ = flush_tick.tick() => {
                if !batch.is_empty()
                    && let Err(e) = flush(&mut sink, &channel, counters, gaps, &mut batch, last_offset, &mut committed, &consumer, &mut hist).await
                {
                    break Err(e);
                }
            }
            _ = metrics_tick.tick() => {
                if tokio::time::timeout(
                    consume::PROBE_TIMEOUT,
                    telemetry::publish_metrics(&channel, "stream", counters, gaps, &mut hist, committed),
                )
                .await
                .is_err()
                {
                    break Err(anyhow::anyhow!("metrics publish timed out; connection presumed dead"));
                }
            }
            _ = probe_tick.tick() => {
                if let Err(e) = consume::probe_liveness(&channel).await {
                    break Err(e);
                }
            }
        }
    };
    let _ = consumer.handle().close().await;
    ctrl.abort();
    result
}

/// Commit (data + offset, one transaction), account, then store the offset
/// broker-side for observability: strictly in that order.
#[allow(clippy::too_many_arguments)]
async fn flush(
    sink: &mut Sink,
    channel: &lapin::Channel,
    counters: &mut Counters,
    gaps: &mut GapTracker,
    batch: &mut Vec<Frame>,
    last_offset: u64,
    committed: &mut Option<u64>,
    consumer: &Consumer,
    hist: &mut Histogram<u64>,
) -> anyhow::Result<()> {
    let inserted = tokio::time::timeout(
        consume::FLUSH_TIMEOUT,
        sink.insert_batch_with_offset(batch, STREAM_CONSUMER_NAME, last_offset),
    )
    .await
    .map_err(|_| anyhow::anyhow!("batch insert timed out; database connection presumed dead"))??;
    let now = now_ms();
    counters.inserted += inserted;
    counters.dup_dropped += batch.len() as u64 - inserted;
    counters.batches += 1;
    *committed = Some(last_offset);

    for frame in batch.iter() {
        hist.saturating_record(now.saturating_sub(frame.t_ms).max(1));
        for change in gaps.observe(frame.pad, frame.well, frame.epoch, frame.seq, now) {
            telemetry::publish_gap_event(channel, &change).await;
        }
    }

    if let Err(e) = consumer.store_offset(last_offset).await {
        warn!(error = %e, "broker-side offset store failed (observability only; the transactional offset is the truth)");
    }
    batch.clear();
    Ok(())
}
