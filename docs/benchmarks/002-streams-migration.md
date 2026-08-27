# 002: The Streams migration, A/B against baseline 001

**Date**: 2026-08-27 · **Phase**: 3 · **Mode**: `stream` (native protocol
both ends; dual-bound stream per spec 008)

## Setup

Identical to benchmark 001 (same machine, same workload shape, same batch
limits) except the data plane: the edge publishes straight to `cb.frames.s`
over the stream protocol as a named dedup producer in 128-message batches;
the ingest consumes offsets with the committed offset stored transactionally
with each batch. Confirm window 256 both modes.

## Load steps (stream mode)

| Step | Target f/s | Generated f/s | Confirmed f/s | Inserted f/s | p50 ms | p99 ms (median) | p99 ms (worst) |
|---|---|---|---|---|---|---|---|
| 1x | 320 | 320 | 320 | 318 | 86 | 183 | 216 |
| 10x | 3,200 | 3,201 | 3,201 | 3,200 | 68 | 164 | 167 |
| 50x | 16,000 | 16,000 | 16,000 | 16,004 | 25 | 44 | 46 |
| 100x | 32,000 | 32,000 | 31,999 | 32,001 | 19 | 35 | 61 |

## The A/B that motivated the migration (100x, 32,000 f/s target)

| | Classic (benchmark 001) | Stream (this run) |
|---|---|---|
| Confirmed throughput | 20,763 f/s (saturated) | **31,999 f/s (full target)** |
| p50 / p99 | 15,207 ms / 15,343 ms | **19 ms / 35 ms** |
| Edge buffer at end | 723,489 frames | 0 |
| Data lost | 0 | 0 |

The classic ceiling was the per-message AMQP publish/confirm round-trip
pipeline; batched stream publishing removes it. Broker and TimescaleDB were
never the bottleneck in either mode (the sink absorbed 32k rows/s here).
At low load, latency is batching-dominated and near-identical in both
modes: the migration buys headroom, not baseline latency.

## Semantics diff, measured not recited

| Concern | Classic queue | Stream |
|---|---|---|
| "What did we commit" | Broker's unacked ledger (ack after DB commit) | Explicit offset, committed **in the same DB transaction** as its batch |
| Crash recovery | Redelivery of unacked; duplicates absorbed by the sink (observed as `dup_dropped` > 0 on restart) | Resume at stored offset + 1; **zero re-read** observed (`dup_dropped` stayed 0 across the kill drill) |
| Consumer restart accounting | Redelivered flags visible | Offset continuity: committed 17,612 before kill, drained to 21,668 after, no gaps, no missing |
| Duplicate publishes | Sink `ON CONFLICT` only | Broker-level dedup (named producer, publishing ids, retransmits reuse ids) **and** the sink; injected dups take fresh ids to keep the sink's layer demonstrable |
| Poison | Reject to DLQ (destructive routing) | Counted and skipped: reads are non-destructive; a DLQ is a queue concept, not a guarantee primitive |
| Reads | Destructive (consumed = gone) | Replayable from any offset/timestamp |
| Retention | "Consumed = gone" | Size/time policy (2 GB here) |
| Ordering | Per-queue FIFO until redelivery/competing consumers | Total order in the stream, offset-monotonic |

## Replay: the disaster-recovery drill

With ~3.4M records retained in the stream, `TRUNCATE frames` + delete the
stored offset + restart the consumer (start position `first`):

- **3.42M rows re-materialized in 68 seconds: 50,280 rows/s sustained,
  57,018 rows/s peak** (insert-timestamp bucketing over the burst), then a
  seamless hand-off to the live tail. The database was rebuilt from the
  broker, end to end, with one restart flag.
- During replay the e2e "latency" metric honestly reports minutes (it
  measures against original event time); it collapses back to normal the
  moment the consumer reaches the live tail.
- Limit demonstrated too: replay reaches only as far back as the stream
  existed. Rows from before the stream's creation are not recoverable from
  it: retention starts when the stream does.
