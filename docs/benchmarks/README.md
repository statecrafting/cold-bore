# Benchmarks

Every phase lands a measured entry here (CLAUDE.md, "numbers discipline"):
a performance claim without an entry does not merge.

## Method (applies to every entry unless it says otherwise)

- **Environment**: everything on one machine (Apple Silicon laptop), broker
  and database in Docker Desktop, services as native release builds. That
  makes absolute numbers lab-bound; the *shape* of the curves and the
  before/after deltas are the findings.
- **Workload**: the standard pad simulation (`CB_PADS x CB_WELLS_PER_PAD`
  wells at `CB_RATE_HZ x multiplier` per well), JSON frames (~150 bytes),
  persistent delivery, publisher confirms on.
- **Protocol**: 30 s warmup, 60 s measurement window per load step unless
  stated. Rates read from the services' cumulative counters (differenced),
  queue depth and unacked from the management API, end-to-end latency from
  the ingest histogram (event time to insert-commit, same host, so no clock
  skew term).
- **Latency honesty**: the ingest batches up to `CB_BATCH_MAX_MS` (default
  200 ms), so that much of every p99 is design, not degradation; entries
  state the batching configuration alongside the numbers.

## Entries

| # | Entry | Phase | Question it answers |
|---|---|---|---|
| 001 | `001-baseline-classic.md` | 1 | What does the straight-through pipeline do at 1x/10x/50x on classic queues? |
