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

## 4. Out of scope

Stream-mode consumption and offset management (phase 3 amends this spec);
competing-consumer scaling (documented follow-on, architecture doc §14).
