---
id: "005-ingest-consumer"
title: "Ingest consumer: ack-after-commit, idempotent batched sink, gap accounting"
status: approved
created: "2026-08-27"
summary: >
  The consumer side: manual-ack consumption from the durable classic queue
  under bounded prefetch, batched idempotent inserts into the frames
  hypertable, ack strictly after commit, poison dead-lettering, per-well gap
  tracking with heal events, and per-second end-to-end latency histograms.
  The data path contains no test-only branches.
establishes:
  - "crates/coldbore-ingest/"
depends_on:
  - "001-architecture"
  - "002-telemetry-model"
  - "003-infra"
---

# 005: Ingest consumer

## 1. Purpose

Turn at-least-once delivery into exactly-once storage, observably: every
duplicate absorbed is counted, every gap opened and healed is an event,
every frame's end-to-end latency lands in a histogram.

## 2. Territory

`crates/coldbore-ingest/`: `consume` (the classic-mode loop and topology
declaration), `sink` (batched UNNEST insert, ON CONFLICT DO NOTHING),
`gap` (per-well range tracking), `control` (kill drill), `telemetry`
(counters).

## 3. Behavior

- **Ack-after-commit, always.** A delivery is settled positively only after
  the database commit containing its frame returns; the batch then
  multiple-acks up to its highest delivery tag. A crash between commit and
  ack yields redelivery, absorbed by the sink. This ordering is the spec's
  central clause; no change may weaken it.
- **Batching.** Flush at `CB_BATCH_MAX_FRAMES` (default 500) or
  `CB_BATCH_MAX_MS` (default 200 ms), whichever first, via one multi-row
  `INSERT ... unnest ... ON CONFLICT (pad_id, well_id, epoch, seq, time)
  DO NOTHING`. Rows-affected shortfall is the duplicate count
  (`dup_dropped`).
- **Poison policy.** Unparseable or invalid frames are rejected without
  requeue and dead-letter to `cb.frames.dlq`; the loop continues. Poison
  never wedges, never crashes, never blocks the batch.
- **Prefetch is the backpressure valve** (`CB_PREFETCH`, default 512): the
  broker holds the backlog, not this process's memory.
- **Gap accounting.** Per `(pad, well)`, scoped to the current producer
  epoch: highest contiguous seq + bounded open ranges (max 1024 per well,
  then summary-counted). Jumps open gaps (`gap_opened` events), late
  arrivals split/shrink ranges, an emptied range emits `gap_healed` with
  heal latency. A newer epoch resets the well's accounting (the dead
  generation's buffers cannot heal anything); stragglers from an older
  epoch are ignored. Accounting state survives reconnects, and at session
  start it is **seeded from the durable store** (newest epoch + max seq per
  well) so a restarted consumer never reports already-landed history as
  open gaps.
- **Latency.** `insert-commit wall clock - frame t_ms` recorded per frame
  in an HDR histogram, drained each metrics tick into p50/p95/p99/max.
- Topology (exchanges, DLX pair, frames queue with dead-letter argument) is
  declared idempotently at session start; the queue is durable, messages
  persistent.
- On broker or database failure the session returns to the supervisor and
  reconnects with capped backoff; unacked deliveries redeliver and are
  absorbed.
- **A dead connection must never be mistaken for an empty queue.** A
  half-dead socket (the post-host-sleep signature) raises no library
  error, and an idle consumer has no traffic to notice it by. The session
  therefore enforces liveness itself: a 5 s passive-declare probe (a real
  broker round trip, time-bounded), a bounded metrics publish, and a
  30 s bound on the batch insert (a batch is milliseconds of work; the
  bound firing means a dead database socket). Any of them failing ends
  the session so the supervisor reconnects. Applies to both classic and
  stream loops; in stream mode the probe rides the AMQP side, and
  breaking the session rebuilds the stream consumer with it.

- **Stream mode** (spec 008 amendment): `CB_MODE=stream` consumes the
  stream via the native protocol; the committed offset is stored in the
  same database transaction as its batch (`stream_offsets`), making
  restart resume exact (no re-read window). Broker-side offset storage is
  best-effort observability. Poison is counted and skipped (reads are
  non-destructive; there is no DLQ). Replay: `CB_STREAM_FORCE_FROM` +
  `CB_STREAM_FROM` re-materialize from any position; the idempotent sink
  absorbs any overlap.

## 4. Out of scope

Competing-consumer scaling (documented follow-on, architecture doc §14);
superstream partitioning.
