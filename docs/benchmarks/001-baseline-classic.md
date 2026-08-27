# 001: Baseline, classic queues, straight-through pipeline

**Date**: 2026-08-27 · **Phase**: 1 · **Mode**: `classic`

## Setup

- Apple Silicon laptop; RabbitMQ 4.1.8 and TimescaleDB (PG17, TSDB 2.29.2)
  in Docker Desktop; `coldbore-edge` / `coldbore-ingest` native release
  builds; api + dashboard running throughout.
- Workload: 4 pads x 8 wells = 32 wells at 10 Hz x multiplier; JSON frames,
  persistent delivery, publisher confirms, confirm window 256, prefetch 512,
  batch limits 500 frames / 200 ms.
- Protocol: 20 s warmup, 45 s measurement per step (60 s at 100x), rates from
  cumulative counters, depth/unacked from the management API, latency from
  the ingest HDR histogram (event time to insert commit, same host).

## Load steps

| Step | Target f/s | Generated f/s | Confirmed f/s | Inserted f/s | p50 ms | p99 ms (median) | p99 ms (worst) | Max depth | Max unacked | Avg batch |
|---|---|---|---|---|---|---|---|---|---|---|
| 1x | 320 | 320 | 321 | 320 | 94 | 196 | 213 | 96 | 96 | 64 |
| 10x | 3,200 | 3,200 | 3,200 | 3,194 | 67 | 165 | 174 | 352 | 352 | 320 |
| 50x | 16,000 | 16,000 | 16,000 | 16,004 | 26 | 49 | 58 | 617 | 512 | 458 |
| 100x | 32,000 | 31,999 | 20,763 | 20,764 | 15,207 | 15,343 | 22,879 | 881 | 512 | 471 |

## Findings

1. **The pipeline sustains 16,000 frames/s end to end** (50x) with no
   backlog: queue depth peaked at 617 with unacked pinned at the prefetch
   cap (512), and TimescaleDB absorbed 16k rows/s in ~460-row UNNEST batches
   without drift.
2. **Latency improves with load, and that is the batching contract made
   visible.** At 1x a 500-frame batch never fills, so most frames wait out
   the 200 ms flush timer (avg batch 64, p99 196 ms). At 50x batches fill by
   count in ~30 ms (p99 49 ms). The p99 at low load is a *configuration*,
   not a degradation: `CB_BATCH_MAX_MS` is the knob, and the sub-50 ms p99
   at 16k f/s is the pipeline's actual latency floor plus batch-fill time.
3. **The ceiling is the producer's confirm path, not the broker or the
   database.** At 100x (32k f/s target) the publish/confirm pipeline
   saturates at ~20.7k f/s on one channel with a 256-confirm window; the
   broker and sink keep absorbing everything that arrives (depth stayed
   under 900). The excess accumulated in the edge's store-and-forward buffer
   (723k frames at measurement end) and drained after the step: **zero
   loss**, latency honestly reflecting the backlog (p50 ~15 s during
   saturation). Known levers, in the order a production system would pull
   them: larger confirm window, multiple publisher channels/connections,
   batch publishing, binary encoding (JSON encode cost is on this path), and
   ultimately the stream protocol (phase 3 measures that directly).
4. **Backpressure behaves as designed end to end**: prefetch caps consumer
   in-flight, the confirm window caps producer in-flight, and the bounded
   edge buffer absorbs what the wire cannot take, with the loss accounting
   (`buffer_dropped`) staying at zero throughout.

## Drills run against this build (phase-1 verification)

| Drill | Result |
|---|---|
| Pad uplink severed 18 s | 1,432 frames buffered (`generated - confirmed` matched exactly); drained in order on restore; 0 missing, 0 gaps |
| 20% duplicate injection | 1,176 duplicates injected, all absorbed by `ON CONFLICT` (`consumed - inserted` = injected); row counts unaffected |
| Reorder window 64 | 1,301 gaps opened, 1,301 healed, 0 open at rest; 0 missing; ordering restored at rest by `seq` |
| Consumer killed under load | Backlog grew to ~4,000; restart drained it in seconds; redeliveries absorbed; 0 missing |
| 100x volume surge | See finding 3; buffer drained post-surge with 0 missing over the surge window |
| Consumer restart (post-epoch build) | Gap accounting seeded from DB watermarks: 0 phantom open gaps |
| Producer restart (post-epoch build) | New producer epoch; seq restart at 1 collides with nothing; 2 epochs in completeness, 0 missing |

Note: the load steps were measured on the pre-epoch build (frames without the
`epoch` field, sink key `(pad, well, seq, time)`). The epoch adds one BIGINT
per row and one array per batch; re-measurement is folded into the phase 3
A/B run rather than repeated here.

## Follow-up found by these drills (shipped in the same phase)

The first consumer-restart drill exposed 32 phantom "open gaps" (an
in-memory tracker reset treating already-landed history as missing), and a
producer restart would have reused seqs from 1 against the same sink key.
Fix: a **producer epoch** on every frame (the job Kafka producer epochs and
stream publisher ids do), epoch-scoped gap accounting, and startup seeding
of the tracker from the durable store's per-well watermarks.
