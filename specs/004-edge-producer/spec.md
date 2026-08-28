---
id: "004-edge-producer"
title: "Edge producer: simulation, confirmed publishing, store-and-forward, fault hooks"
status: approved
created: "2026-08-27"
summary: >
  The field side: N pads x M wells of generated telemetry published with
  publisher confirms; per-pad bounded store-and-forward buffers for severed
  uplinks; the confirm/retransmit window; and every injectable fault (link,
  dup, reorder, rate) in the publish path. At-least-once from the edge:
  a frame leaves edge custody only on broker confirm.
establishes:
  - "crates/coldbore-edge/"
depends_on:
  - "001-architecture"
  - "002-telemetry-model"
---

# 004: Edge producer

## 1. Purpose

Model the remote-field reality the platform exists to survive: sensors that
never stop sampling, an uplink that fails, and a broker that must be told
the truth about what was and was not delivered.

## 2. Territory

`crates/coldbore-edge/`: `sim` (waveform generation, seq assignment),
`uplink` (buffers, confirm window, reorder/dup application, reconnect),
`faults` (injectable state), `control` (command consumer), `telemetry`
(counters, snapshot task).

## 3. Behavior

- **Generation never pauses.** The generator ticks at
  `rate_hz x multiplier` per well and hands every frame to the uplink;
  publishability is the uplink's problem. Seq is assigned at generation,
  monotonically per `(pad, well)`.
- **Custody rule.** Publisher confirms are on; a frame is retained (retry
  queue) until the broker acks it. Nack, publish error, or connection loss
  requeues it. A confirm lost in transit produces a duplicate publish;
  that is correct behavior, absorbed downstream.
- **Store-and-forward.** Every frame enters its pad queue; a pad with
  `link down` (or a dead broker connection) simply stops draining. Buffers
  are bounded (`CB_BUFFER_CAP` per pad); at capacity the oldest frame drops,
  counted in `buffer_dropped`. Drain preserves seq order; buffered frames
  publish before newer live frames of the same pad.
- **Faults live here only** (plus the process supervisor): `dup` re-publishes
  a confirmed frame with probability `rate`; `reorder` shuffles windows of
  `window` frames; `rate` scales generation; `kill edge` exits 3. `reset`
  restores all fault defaults. Every applied fault emits a `fault_applied`
  event.
- **The field resizes live.** `topology { pads, wells_per_pad }` takes
  effect on the next generation tick: new wells start their own seq
  timeline within the current epoch, existing wells are never reset, and a
  well resized away and back resumes its own counter (seqs never reused).
  Topology is a setting: `reset` keeps it; `CB_PADS`/`CB_WELLS_PER_PAD`
  remain the boot defaults. Pads gained come up with a healthy link.
- **Publishes are persistent** (delivery mode 2) with `message_id =
  pad-well-seq`; frames JSON per spec 002.
- **The edge declares the full frames topology** (exchanges, DLX pair,
  durable frames queue, binding), byte-identical to the ingest's
  declaration: a frame published before the consumer's first start must
  land in the durable queue, never be confirmed-but-unroutable (that would
  be silent loss).
- Reconnection uses capped exponential backoff (0.5 s to 10 s); frames
  arriving while disconnected buffer per pad; unconfirmed in-flight frames
  are reaped into the retry queue before the session ends.
- **A dead connection must never be mistaken for a quiet one.** A
  half-dead socket (the post-host-sleep signature) raises no library
  error, so the uplink enforces liveness itself: publishes and the
  teardown reap are time-bounded; confirms making no progress for 15 s
  while frames are in flight end the session; and a 5 s passive-declare
  probe (a real broker round trip) covers the idle case. Custody is held
  outside the confirm futures so an unresolved confirm can never strand
  its frame: at teardown, unresolved frames go to the retry queue
  (possible duplicate publish, absorbed by the sink). The same stall
  bound applies to the stream publisher (30 s without a confirm tears the
  session down rather than cycling retransmits forever).
- **Partial death is process death.** If the generator or uplink task ends
  outside a ctrl-c, the process exits non-zero so the supervisor restarts
  it; a process with a dead core task must not linger looking alive.

- **Stream mode** (spec 008 amendment): `CB_MODE=stream` publishes
  straight to the stream in batches over the native protocol as a named
  dedup producer (`cb-edge-{epoch}`); publishing ids are monotonic,
  retransmissions reuse their id (broker-level dedup of confirm-loss
  duplicates), injected dups take fresh ids, and confirmations presumed
  lost sweep back to retransmission after 10 s. Custody rules are
  identical to classic mode; the AMQP connection remains for control,
  telemetry, and topology.

## 4. Out of scope

Disk-backed buffers (decision log §14); waveform realism beyond plausible
ranges; superstream partitioning.
