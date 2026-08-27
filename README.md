# cold-bore

**A frac-pad telemetry pipeline you can break on purpose.**

cold-bore is an interactive, fault-injectable scale model of a real-time
completions-style data platform:

```
field (simulated pads) → edge buffer → RabbitMQ → Rust ingest → TimescaleDB → FastAPI/WebSocket → dashboard
```

Synthetic frac-pad sensors (pump pressure, slurry rate, proppant
concentration, wellhead temperature) stream sub-second telemetry through a
real distributed pipeline. The dashboard doubles as a game console: you are
the night-shift infrastructure engineer. Sever a pad's uplink, inject
duplicates, shuffle message order, kill the consumer mid-batch, 20x the
volume, and watch the system degrade, recover, and account for every frame.

The pipeline is **at-least-once end to end, effectively exactly-once at the
sink** via idempotent batched inserts, and every claim about it is a number
you can reproduce: throughput ceilings, p99 end-to-end latency, consumer
lag, recovery time, completeness after a drill.

The second act is a **RabbitMQ classic-queues → Streams migration lab**: the
same workload and the same failure drills run on both transports, with the
semantic differences (acks vs offsets, destructive vs replayable reads,
retention, single-active-consumer) measured and written up.

## Status

Phase 0: governance and architecture. See
[`docs/design/architecture.md`](docs/design/architecture.md) for the full
design (components, delivery contract hop by hop, failure drill matrix,
migration plan, phasing).

## Stack

- **Rust**: edge simulator/publisher and ingest consumer (the hot path)
- **RabbitMQ 4**: classic queues and Streams, plus control and telemetry planes
- **TimescaleDB** (PostgreSQL 17): hypertables, continuous aggregates, compression
- **Python**: FastAPI REST + WebSocket egress, scenario engine
- **spec-spine**: the repo governs itself; every component is owned by a spec
  under [`specs/`](specs/), and CI refuses code that drifts from its owning spec

## Not affiliated

This project models the *architecture shape* of real-time completions
platforms. All data is synthetic; it is not affiliated with and contains
nothing from any real product.
