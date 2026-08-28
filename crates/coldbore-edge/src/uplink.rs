//! The uplink: everything between "a frame exists" and "the broker confirmed
//! it". Owns the per-pad store-and-forward buffers, the confirm/retransmit
//! window, duplicate and reorder injection, and broker reconnection.
//!
//! Delivery contract (architecture doc §5): a frame leaves edge custody only
//! on broker confirm. Nack, publish error, or connection loss puts it back in
//! line; a confirm lost in transit yields a duplicate publish, absorbed by
//! the idempotent sink. Per-pad order is preserved through buffering and
//! drain; only the reorder fault (deliberately) breaks it.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use coldbore_proto::Frame;
use coldbore_proto::config::EdgeConfig;
use coldbore_proto::metrics::event_kind;
use coldbore_proto::topology::FRAMES_EXCHANGE;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, ExchangeKind,
    PublisherConfirm,
};
use rand::RngExt;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{info, warn};

use crate::control;
use crate::faults::FaultBox;
use crate::telemetry::{self, Counters};

/// State that survives reconnects: frames in edge custody. Shared by the
/// classic (AMQP) and stream (native protocol) publishers: custody is
/// transport-neutral.
pub(crate) struct UplinkState {
    /// Per-pad uplink queues; store-and-forward is these queues not draining.
    pad_buffers: BTreeMap<u16, VecDeque<Frame>>,
    /// Frames that failed a publish or were nacked: front of the line.
    pub(crate) retry: VecDeque<Frame>,
    /// Injected duplicates awaiting publish.
    pub(crate) dup_queue: VecDeque<Frame>,
    /// Reorder fault: frames held until the shuffle window fills.
    shuffle: Vec<Frame>,
    /// Frames staged for publish (post-reorder).
    publish_queue: VecDeque<Frame>,
    buffer_cap: usize,
    pub(crate) backoff: Duration,
}

pub(crate) const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(10);

impl UplinkState {
    pub(crate) fn new(cfg: &EdgeConfig) -> Self {
        Self {
            pad_buffers: (1..=cfg.pads).map(|p| (p, VecDeque::new())).collect(),
            retry: VecDeque::new(),
            dup_queue: VecDeque::new(),
            shuffle: Vec::new(),
            publish_queue: VecDeque::new(),
            buffer_cap: cfg.buffer_cap,
            backoff: INITIAL_BACKOFF,
        }
    }

    /// Every live frame enters its pad queue; publishability is decided at
    /// drain time. Bounded: at capacity the oldest frame drops (counted),
    /// architecture doc §5.2.
    pub(crate) fn buffer_frame(&mut self, frame: Frame, counters: &Counters) {
        let buf = self.pad_buffers.entry(frame.pad).or_default();
        if buf.len() >= self.buffer_cap {
            buf.pop_front();
            counters.buffer_dropped.fetch_add(1, Relaxed);
        }
        buf.push_back(frame);
    }

    /// Frames currently in store-and-forward custody.
    pub(crate) fn buffered(&self) -> u64 {
        self.pad_buffers
            .values()
            .map(|b| b.len() as u64)
            .sum::<u64>()
            + self.shuffle.len() as u64
            + self.publish_queue.len() as u64
            + self.retry.len() as u64
    }

    /// Pull the next frame to put on the wire. Priority: retransmits, then
    /// injected duplicates, then pad queues (link-up pads only) through the
    /// reorder stage.
    pub(crate) fn next_publishable(&mut self, faults: &FaultBox) -> Option<(Frame, bool)> {
        if let Some(f) = self.retry.pop_front() {
            return Some((f, false));
        }
        if let Some(f) = self.dup_queue.pop_front() {
            return Some((f, true));
        }
        if self.publish_queue.is_empty() {
            self.fill_publish_queue(faults);
        }
        self.publish_queue.pop_front().map(|f| (f, false))
    }

    fn fill_publish_queue(&mut self, faults: &FaultBox) {
        let window = faults.reorder_window();
        let mut pulled = 0_usize;
        loop {
            let mut any = false;
            for (&pad, buf) in self.pad_buffers.iter_mut() {
                if !faults.link_up(pad) {
                    continue;
                }
                if let Some(frame) = buf.pop_front() {
                    self.shuffle.push(frame);
                    any = true;
                    pulled += 1;
                }
            }
            if !any || pulled >= 512 {
                break;
            }
        }
        if window <= 1 {
            // Reorder off: pass frames through in arrival order.
            self.publish_queue.extend(self.shuffle.drain(..));
        } else if self.shuffle.len() >= window as usize {
            let mut rng = rand::rng();
            for i in (1..self.shuffle.len()).rev() {
                self.shuffle.swap(i, rng.random_range(0..=i));
            }
            self.publish_queue.extend(self.shuffle.drain(..));
        }
    }
}

/// Supervisor: connect, serve until the connection dies, buffer while
/// disconnected, reconnect with capped exponential backoff. Never loses a
/// frame silently in between.
pub async fn run(
    cfg: EdgeConfig,
    faults: FaultBox,
    counters: Arc<Counters>,
    mut rx: Receiver<Frame>,
) {
    let mut st = UplinkState::new(&cfg);
    loop {
        match connect_and_serve(&cfg, &faults, &counters, &mut rx, &mut st).await {
            Ok(()) => return, // generator gone: shutdown
            Err(e) => {
                warn!(error = %e, backoff_ms = st.backoff.as_millis() as u64, "uplink connection lost")
            }
        }
        // While disconnected, keep accepting frames into buffers.
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

async fn connect_and_serve(
    cfg: &EdgeConfig,
    faults: &FaultBox,
    counters: &Arc<Counters>,
    rx: &mut Receiver<Frame>,
    st: &mut UplinkState,
) -> anyhow::Result<()> {
    let conn = Connection::connect(
        &cfg.common.amqp_url,
        ConnectionProperties::default().with_connection_name("coldbore-edge".into()),
    )
    .await?;
    let channel = conn.create_channel().await?;
    declare_topology(&channel).await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;
    info!("uplink connected");
    st.backoff = INITIAL_BACKOFF;

    let ctrl_channel = conn.create_channel().await?;
    let ctrl = tokio::spawn(control::run(ctrl_channel, faults.clone()));
    let met_channel = conn.create_channel().await?;
    let met = tokio::spawn(telemetry::metrics_task(
        met_channel,
        counters.clone(),
        faults.clone(),
        cfg.clone(),
    ));
    let _ = telemetry::publish_event(
        &channel,
        event_kind::SERVICE_STARTED,
        "edge",
        serde_json::json!({"buffered": st.buffered()}),
    )
    .await;

    let result = publish_loop(cfg, faults, counters, rx, st, &channel).await;
    ctrl.abort();
    met.abort();
    result
}

/// The edge declares the FULL frames topology (exchanges, DLX pair, and the
/// durable frames queue with its binding), not just the exchanges it
/// publishes to. Without the queue, frames published before the ingest's
/// first start would be confirmed by the broker yet unroutable: silent loss,
/// the one unforgivable bug. Declarations are idempotent; the arguments here
/// MUST stay byte-identical to the ingest's declaration in
/// crates/coldbore-ingest/src/consume.rs (a mismatch is a broker
/// PRECONDITION_FAILED at startup).
pub(crate) async fn declare_topology(channel: &Channel) -> lapin::Result<()> {
    use coldbore_proto::topology::{
        CONTROL_EXCHANGE, FRAMES_BINDING, FRAMES_DLQ, FRAMES_DLX, FRAMES_QUEUE, FRAMES_STREAM,
        TELEMETRY_EXCHANGE,
    };
    use lapin::options::{QueueBindOptions, QueueDeclareOptions};
    use lapin::types::AMQPValue;

    let durable = ExchangeDeclareOptions {
        durable: true,
        ..ExchangeDeclareOptions::default()
    };
    channel
        .exchange_declare(
            FRAMES_EXCHANGE.into(),
            ExchangeKind::Topic,
            durable,
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            CONTROL_EXCHANGE.into(),
            ExchangeKind::Fanout,
            durable,
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            TELEMETRY_EXCHANGE.into(),
            ExchangeKind::Topic,
            durable,
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            FRAMES_DLX.into(),
            ExchangeKind::Fanout,
            durable,
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

    // The stream, bound alongside the classic queue: every frame published
    // to the exchange lands in both transports, so consumers can migrate
    // (and replay) with zero producer change. Spec 008.
    let mut stream_args = FieldTable::default();
    stream_args.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("stream".into()),
    );
    stream_args.insert(
        "x-max-length-bytes".into(),
        AMQPValue::LongLongInt(2_000_000_000),
    );
    channel
        .queue_declare(FRAMES_STREAM.into(), durable_queue, stream_args)
        .await?;
    channel
        .queue_bind(
            FRAMES_STREAM.into(),
            FRAMES_EXCHANGE.into(),
            FRAMES_BINDING.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

type ConfirmFuture = BoxFuture<'static, lapin::Result<Confirmation>>;

/// A publish that does not complete within this bound means the socket is
/// gone (a post-sleep half-dead connection buffers writes forever instead
/// of erroring).
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);
/// Frames in flight but zero confirms arriving for this long: the
/// connection is presumed dead even though lapin has raised no error.
const CONFIRM_STALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Cadence and bound of the liveness probe (a passive queue declare, i.e. a
/// real broker round trip) that catches dead connections even while the
/// publish path is idle.
const PROBE_INTERVAL: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a teardown waits for straggler confirms before declaring the
/// rest unconfirmed and queueing them for retransmission.
const REAP_TIMEOUT: Duration = Duration::from_secs(3);

async fn publish_loop(
    cfg: &EdgeConfig,
    faults: &FaultBox,
    counters: &Arc<Counters>,
    rx: &mut Receiver<Frame>,
    st: &mut UplinkState,
    channel: &Channel,
) -> anyhow::Result<()> {
    let mut inflight: FuturesOrdered<ConfirmFuture> = FuturesOrdered::new();
    // Custody stays here, not inside the confirm futures: FuturesOrdered
    // yields in push order, so the front of this deque always pairs with
    // the next yielded confirm, and a confirm that never resolves can never
    // strand its frame.
    let mut pending: VecDeque<(Frame, bool)> = VecDeque::new();
    let mut drain_tick = tokio::time::interval(Duration::from_millis(20));
    drain_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut probe_tick = tokio::time::interval(PROBE_INTERVAL);
    probe_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_progress = Instant::now();

    loop {
        // Pump: put frames on the wire up to the confirm window.
        while inflight.len() < cfg.confirm_window {
            let Some((frame, is_dup)) = st.next_publishable(faults) else {
                break;
            };
            match tokio::time::timeout(PUBLISH_TIMEOUT, start_publish(channel, &frame)).await {
                Ok(Ok(confirm)) => {
                    counters.published.fetch_add(1, Relaxed);
                    pending.push_back((frame, is_dup));
                    inflight.push_back(Box::pin(confirm));
                }
                Ok(Err(e)) => {
                    st.retry.push_front(frame);
                    reap_all(&mut inflight, &mut pending, st, counters).await;
                    return Err(e.into());
                }
                Err(_) => {
                    st.retry.push_front(frame);
                    reap_all(&mut inflight, &mut pending, st, counters).await;
                    return Err(anyhow::anyhow!(
                        "publish timed out after {PUBLISH_TIMEOUT:?}; connection presumed dead"
                    ));
                }
            }
        }
        counters.buffered.store(st.buffered(), Relaxed);

        tokio::select! {
            biased;
            reaped = inflight.next(), if !inflight.is_empty() => {
                // FuturesOrdered guarded by is_empty: next() is Some.
                let Some(result) = reaped else { continue };
                let (frame, is_dup) = pending.pop_front().expect("pending tracks inflight 1:1");
                last_progress = Instant::now();
                match result {
                    Ok(c) if c.is_ack() => {
                        counters.confirmed.fetch_add(1, Relaxed);
                        if !is_dup {
                            let p = faults.dup_rate();
                            if p > 0.0 && rand::rng().random::<f64>() < p {
                                counters.dup_injected.fetch_add(1, Relaxed);
                                st.dup_queue.push_back(frame);
                            }
                        }
                    }
                    Ok(_) => {
                        // Broker nack: the frame is not safe; retransmit.
                        counters.retransmits.fetch_add(1, Relaxed);
                        if !is_dup {
                            st.retry.push_back(frame);
                        }
                    }
                    Err(e) => {
                        if !is_dup {
                            st.retry.push_back(frame);
                        }
                        reap_all(&mut inflight, &mut pending, st, counters).await;
                        return Err(e.into());
                    }
                }
            }
            received = rx.recv() => {
                match received {
                    Some(frame) => st.buffer_frame(frame, counters),
                    None => {
                        reap_all(&mut inflight, &mut pending, st, counters).await;
                        return Ok(());
                    }
                }
            }
            _ = drain_tick.tick() => {}
            _ = probe_tick.tick() => {
                if inflight.is_empty() {
                    last_progress = Instant::now();
                } else if last_progress.elapsed() > CONFIRM_STALL_TIMEOUT {
                    let stalled = pending.len();
                    reap_all(&mut inflight, &mut pending, st, counters).await;
                    return Err(anyhow::anyhow!(
                        "no confirm progress for {CONFIRM_STALL_TIMEOUT:?} with {stalled} in flight; connection presumed dead"
                    ));
                }
                if let Err(e) = probe_liveness(channel).await {
                    reap_all(&mut inflight, &mut pending, st, counters).await;
                    return Err(e);
                }
            }
        }
    }
}

/// A passive declare of the frames queue: a cheap synchronous broker round
/// trip. On a healthy connection it returns immediately; on a half-dead one
/// (the post-sleep signature) it hangs or errors, bounding zombie time to
/// `PROBE_INTERVAL + PROBE_TIMEOUT` instead of forever.
pub(crate) async fn probe_liveness(channel: &Channel) -> anyhow::Result<()> {
    use coldbore_proto::topology::FRAMES_QUEUE;
    use lapin::options::QueueDeclareOptions;
    let passive = QueueDeclareOptions {
        passive: true,
        ..QueueDeclareOptions::default()
    };
    match tokio::time::timeout(
        PROBE_TIMEOUT,
        channel.queue_declare(FRAMES_QUEUE.into(), passive, FieldTable::default()),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(anyhow::anyhow!("liveness probe failed: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "liveness probe timed out after {PROBE_TIMEOUT:?}; connection presumed dead"
        )),
    }
}

/// Collect whatever confirms can still resolve within `REAP_TIMEOUT`; every
/// frame whose confirm did not resolve goes back to the retry queue, so no
/// frame is stranded by a dead connection. An unresolved confirm may mean
/// the broker did get the frame: that is a duplicate publish, absorbed by
/// the idempotent sink.
async fn reap_all(
    inflight: &mut FuturesOrdered<ConfirmFuture>,
    pending: &mut VecDeque<(Frame, bool)>,
    st: &mut UplinkState,
    counters: &Counters,
) {
    let deadline = Instant::now() + REAP_TIMEOUT;
    while !inflight.is_empty() {
        match tokio::time::timeout_at(deadline, inflight.next()).await {
            Ok(Some(result)) => {
                let (frame, is_dup) = pending.pop_front().expect("pending tracks inflight 1:1");
                match result {
                    Ok(c) if c.is_ack() => {
                        counters.confirmed.fetch_add(1, Relaxed);
                    }
                    _ => {
                        counters.retransmits.fetch_add(1, Relaxed);
                        if !is_dup {
                            st.retry.push_back(frame);
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(_) => break, // dead connection: stop waiting for confirms
        }
    }
    while let Some((frame, is_dup)) = pending.pop_front() {
        counters.retransmits.fetch_add(1, Relaxed);
        if !is_dup {
            st.retry.push_back(frame);
        }
    }
    *inflight = FuturesOrdered::new();
}

async fn start_publish(channel: &Channel, frame: &Frame) -> lapin::Result<PublisherConfirm> {
    let payload = serde_json::to_vec(frame).unwrap_or_default();
    let props = BasicProperties::default()
        .with_content_type("application/json".into())
        .with_message_id(frame.message_id().into())
        .with_timestamp(frame.t_ms / 1000)
        .with_delivery_mode(2); // persistent
    channel
        .basic_publish(
            FRAMES_EXCHANGE.into(),
            frame.routing_key().into(),
            BasicPublishOptions::default(),
            &payload,
            props,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use coldbore_proto::frame::FRAME_VERSION;

    fn cfg(buffer_cap: usize) -> EdgeConfig {
        EdgeConfig {
            common: coldbore_proto::config::CommonConfig {
                amqp_url: String::new(),
                mode: coldbore_proto::config::Mode::Classic,
                metrics_interval_ms: 1000,
                stream_host: String::new(),
                stream_port: 5552,
                stream_user: String::new(),
                stream_pass: String::new(),
            },
            pads: 2,
            wells_per_pad: 1,
            rate_hz: 10.0,
            buffer_cap,
            confirm_window: 4,
        }
    }

    fn frame(pad: u16, seq: u64) -> Frame {
        Frame {
            v: FRAME_VERSION,
            pad,
            well: 1,
            epoch: 1,
            seq,
            t_ms: seq,
            pressure_psi: 0.0,
            rate_bpm: 0.0,
            proppant_ppa: 0.0,
            temp_f: 0.0,
        }
    }

    #[test]
    fn per_pad_order_preserved_without_faults() {
        let faults = FaultBox::new(2, 1);
        let counters = Counters::default();
        let mut st = UplinkState::new(&cfg(100));
        for seq in 1..=5 {
            st.buffer_frame(frame(1, seq), &counters);
            st.buffer_frame(frame(2, seq * 10), &counters);
        }
        let mut seen_p1 = Vec::new();
        while let Some((f, is_dup)) = st.next_publishable(&faults) {
            assert!(!is_dup);
            if f.pad == 1 {
                seen_p1.push(f.seq);
            }
        }
        assert_eq!(seen_p1, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn link_down_holds_frames_and_cap_drops_oldest() {
        let faults = FaultBox::new(2, 1);
        let counters = Counters::default();
        let mut st = UplinkState::new(&cfg(3));
        faults.apply(&coldbore_proto::ControlCommand::Link {
            pad: 1,
            state: coldbore_proto::LinkState::Down,
        });
        for seq in 1..=5 {
            st.buffer_frame(frame(1, seq), &counters);
        }
        // Nothing publishable: pad 1 is down, pad 2 is empty.
        assert!(st.next_publishable(&faults).is_none());
        assert_eq!(counters.buffer_dropped.load(Relaxed), 2); // cap 3, dropped oldest 2

        faults.apply(&coldbore_proto::ControlCommand::Link {
            pad: 1,
            state: coldbore_proto::LinkState::Up,
        });
        let drained: Vec<u64> =
            std::iter::from_fn(|| st.next_publishable(&faults).map(|(f, _)| f.seq)).collect();
        assert_eq!(drained, vec![3, 4, 5]); // oldest two dropped, order intact
    }

    #[test]
    fn reorder_window_shuffles_but_loses_nothing() {
        let faults = FaultBox::new(1, 1);
        let counters = Counters::default();
        let mut st = UplinkState::new(&cfg(1000));
        faults.apply(&coldbore_proto::ControlCommand::Reorder { window: 8 });
        for seq in 1..=8 {
            st.buffer_frame(frame(1, seq), &counters);
        }
        let mut seqs: Vec<u64> =
            std::iter::from_fn(|| st.next_publishable(&faults).map(|(f, _)| f.seq)).collect();
        assert_eq!(seqs.len(), 8);
        seqs.sort_unstable();
        assert_eq!(seqs, (1..=8).collect::<Vec<_>>());
    }
}
