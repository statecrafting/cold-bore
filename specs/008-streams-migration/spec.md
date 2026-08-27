---
id: "008-streams-migration"
title: "Classic queues to RabbitMQ Streams: the migration lab"
status: approved
created: "2026-08-27"
summary: >
  The volume-driven migration of the frames data plane from the classic
  queue to a RabbitMQ Stream, executed as the incremental path a production
  system would take: dual-bind the stream alongside the queue (zero producer
  change), move the consumer to offset tracking with the offset stored
  transactionally with the data, then move the producer to the native
  stream protocol (a named dedup producer) to lift the confirm-path
  throughput ceiling. Replay becomes a first-class drill.
amends:
  - "003-infra"
  - "004-edge-producer"
  - "005-ingest-consumer"
  - "006-api-service"
  - "007-dashboard"
depends_on:
  - "001-architecture"
  - "002-telemetry-model"
---

# 008: Classic queues to RabbitMQ Streams

## 1. Purpose

Benchmark 001 found the classic pipeline's ceiling in the producer's
confirm path (~20.7k frames/s), with broker and sink still comfortable.
This spec migrates the data plane to Streams the way a live system would:
no flag-day, no producer/consumer lockstep, measured at every step.

## 2. The migration steps (each independently shippable)

1. **Dual-bind** (amends 003, 004, 005): `cb.frames.s` is declared by both
   services as a stream-type queue (`x-queue-type=stream`, 2 GB retention)
   bound `frames.#` to the frames exchange. From that moment every
   AMQP-published frame lands in both transports; the stream accumulates
   history while classic consumption continues untouched.
2. **Consumer migration** (amends 005): `CB_MODE=stream` consumes via the
   native protocol. "What did we commit" moves from the broker's unacked
   ledger to an explicit offset, and the offset is committed **in the same
   database transaction as the batch it covers** (`stream_offsets` table),
   so a crashed consumer resumes exactly where the data ends: no re-read
   window, no gap. The broker-side offset store is updated post-commit as
   observability, never as truth. Poison is counted and skipped (reads are
   non-destructive; there is no DLQ to route into): the dead-letter queue
   is revealed as a queue-transport concept, not a delivery-guarantee
   primitive.
3. **Producer migration** (amends 004): `CB_MODE=stream` publishes straight
   to the stream in batches over the stream protocol, as a **named dedup
   producer** (`cb-edge-{epoch}`: the name embeds the producer generation so
   a restart starts a fresh dedup timeline). Every message carries a
   monotonic publishing id; retransmissions reuse their id, so the broker
   itself absorbs confirm-loss duplicates. Injected duplicates (the fault)
   take fresh ids on purpose: they must reach the sink to demonstrate its
   layer. Custody rules are unchanged: a frame leaves the edge only on
   positive confirmation; unconfirmed frames sweep back to retransmission
   after 10 s.
4. **Egress awareness** (amends 006, 007): the api polls the stream's stats
   alongside the queue's; the dashboard's backlog series becomes
   transport-aware (queue depth in classic, `retained - 1 - committed
   offset` in stream mode, with the mgmt counter's chunk-lag caveat).

## 3. Behavior (binding clauses)

- The delivery contract of specs 004/005 is transport-independent and MUST
  hold identically in both modes; only the mechanism differs (ack-after-
  commit becomes offset-after-commit, in-transaction).
- Replay is a supported operation, not a recovery hack: with
  `CB_STREAM_FORCE_FROM=true` (or no stored offset) the consumer starts at
  `CB_STREAM_FROM` (`first` | `next` | `offset:N`), and the idempotent sink
  makes any overlap harmless. Re-materializing the database from the
  stream is the disaster-recovery drill and is benchmarked.
- Both modes remain first-class: `CB_MODE` selects per service, and A/B
  benchmarks run on one broker.

## 4. Out of scope

Superstreams / partitioning (the documented scaling step beyond one
stream's total order); Kafka egress; retention tiering.
