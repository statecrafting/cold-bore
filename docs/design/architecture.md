# cold-bore: architecture

cold-bore is an interactive, fault-injectable scale model of a real-time
completions-style data platform: simulated frac-pad sensors stream telemetry
from an "edge" process through RabbitMQ into a Rust ingest service that lands
frames in TimescaleDB, with a Python API fanning live data out over WebSocket
to a browser dashboard. The dashboard doubles as a game console: you play the
night-shift infrastructure engineer, injecting faults (link loss, duplicates,
reordering, consumer death, volume surges) and holding the pipeline's SLOs
while it degrades and recovers.

The project exists to study, with first-hand numbers, the things that matter
in high-throughput event pipelines: delivery guarantees, idempotency,
ordering, backpressure, consumer lag, gap healing, and the migration of a
workload from classic RabbitMQ queues to RabbitMQ Streams.

It models the *shape* of a completions data platform (remote field, edge
buffering, sub-second telemetry, time-series storage, customer egress). All
data is synthetic. It is not affiliated with, and contains nothing from, any
real platform.

## 1. System diagram

```
                     ── data plane ──────────────────────────────────────────
  pad simulators                    RabbitMQ
 ┌──────────────┐   confirms   ┌────────────────┐  ack/offset  ┌───────────┐
 │ coldbore-edge├─────────────▶│ cb.frames.x    ├─────────────▶│ coldbore- │
 │  (Rust)      │              │  ├─ cb.frames.q│              │  ingest   │
 │  ┌────────┐  │              │  │   (classic) │              │  (Rust)   │
 │  │ store &│  │              │  └─ cb.frames.s│              │ batch +   │
 │  │ forward│  │              │      (stream)  │              │ idempotent│
 │  └────────┘  │              └────────────────┘              └─────┬─────┘
 └──────────────┘                                                    │ COPY-style
                                                                     ▼ batches
                     ── egress ────────────────────────┐   ┌──────────────┐
 ┌───────────┐   WebSocket + REST   ┌───────────┐      │   │ TimescaleDB  │
 │ dashboard │◀─────────────────────┤ api       │◀─────┴───┤  frames      │
 │ (browser) │                      │ (Python)  │  asyncpg │  hypertable  │
 └───────────┘                      └───────────┘          └──────────────┘

 control plane:    api ──▶ cb.control.x (fanout) ──▶ edge, ingest
 telemetry plane:  edge, ingest ──▶ cb.telemetry.x (topic) ──▶ api ──▶ WS + events table
 broker stats:     api ──▶ RabbitMQ management HTTP API (queue depth, rates)
```

Three planes share one broker:

- **Data plane**: sensor frames, the high-volume path.
- **Control plane**: fault-injection and configuration commands, fanout so
  every service sees every command and applies what addresses it.
- **Telemetry plane**: 1 Hz metrics snapshots and discrete events (gaps,
  heals, faults applied, consumer lifecycle) from every service.

Running metrics over the broker instead of a Prometheus stack is a deliberate
trade-off: the pipeline demonstrates itself, the dashboard needs no scrape
infrastructure, and the metrics stream exercises the same delivery machinery.
The cost (metrics die with the broker) is acceptable in a lab; the design doc
for a production system would say Prometheus.

## 2. Components

| Component | Path | Language | Role |
|---|---|---|---|
| proto | `crates/coldbore-proto` | Rust | Shared vocabulary: frame, control, metrics, event types; broker topology constants; env config |
| edge | `crates/coldbore-edge` | Rust | Pad simulator + publisher: telemetry generation, publisher confirms, store-and-forward, fault hooks |
| ingest | `crates/coldbore-ingest` | Rust | Consumer + sink: classic and stream modes, batched idempotent inserts, gap tracking, latency histograms |
| api | `services/api` | Python | FastAPI: REST + WebSocket egress, control publisher, telemetry consumer, event persistence, scenario engine (phase 4) |
| dashboard | `dashboard/` | JS (no build step) | Live charts, fault console, scenario picker, score display |
| infra | `infra/` | compose/SQL/conf | RabbitMQ + TimescaleDB provisioning, schema migrations |

Dependency direction: `proto ← edge`, `proto ← ingest`. The api reads the
same wire contracts (JSON) but is intentionally decoupled at the type level;
the JSON schemas in this document are the cross-language contract.

## 3. Broker topology

| Object | Kind | Notes |
|---|---|---|
| `cb.frames.x` | topic exchange, durable | routing key `frames.pad{P}.well{W}` |
| `cb.frames.q` | classic queue, durable | bound `frames.#`; DLX to `cb.frames.dlx` |
| `cb.frames.dlx` / `cb.frames.dlq` | fanout exchange + queue | poison frames (unparseable, schema-invalid) |
| `cb.frames.s` | stream | phase 3; native stream protocol (port 5552) |
| `cb.control.x` | fanout exchange, durable | services bind exclusive auto-delete queues |
| `cb.telemetry.x` | topic exchange, durable | `metrics.{service}`, `events.{kind}` |
| `cb.telemetry.api.q` | queue, auto-delete | api's binding to `metrics.#` and `events.#` |

`CB_MODE=classic|stream` selects the data-plane path in edge and ingest. Both
modes coexist in the topology so A/B benchmarks can run back to back on one
broker.

## 4. Frame contract

JSON body (JSON chosen for cross-language legibility and inspectability; the
decision log covers the trade-off):

```json
{
  "v": 1,
  "pad": 2,
  "well": 5,
  "epoch": 1724790000000,
  "seq": 184467,
  "t_ms": 1724790000123,
  "pressure_psi": 8543.2,
  "rate_bpm": 92.4,
  "proppant_ppa": 1.85,
  "temp_f": 74.3
}
```

- `seq` is assigned **only** by the edge, monotonically increasing per
  `(pad, well)` within a producer generation, never reused, never
  reassigned. Together with `epoch` it is the idempotency and gap-detection
  key for the entire pipeline.
- `epoch` is the producer generation: the edge process's start wall-clock in
  ms. A restarted edge starts a new epoch, so seq restarting at 1 can never
  collide with the previous run's rows, gap accounting resets per
  generation, and stragglers from a dead generation are identifiable. (The
  same job Kafka producer epochs and stream publisher ids do.)
- `t_ms` is event time (producer wall clock at sample generation). End-to-end
  latency is measured against it; edge and ingest run on the same host or
  LAN, so clock skew is negligible for lab purposes (noted in the SLO
  section).
- Channels model a frac pad coarsely: treating pressure, slurry rate,
  proppant concentration, and wellhead temperature as smooth processes with
  noise and occasional step changes. Realism of the waveforms is a non-goal;
  realism of the data *rates* is the point.
- AMQP properties: `message_id = "{pad}-{well}-{seq}"`, `timestamp`,
  `content_type = application/json`. Duplicate injection re-publishes the
  identical body and properties.

## 5. Delivery contract, hop by hop

The pipeline is **at-least-once end to end, made effectively exactly-once at
the sink by an idempotent insert**. Every hop states its guarantee:

1. **Edge → broker (classic)**: publisher confirms on. A frame leaves the
   retransmit window only on confirm. Nack, timeout, or connection loss
   requeues it for retransmission in seq order. A confirm lost in transit
   produces a duplicate publish: accepted, the sink absorbs it.
2. **Edge store-and-forward**: `link down` (a control command) simulates a
   severed field uplink. Generation continues (sensors do not stop sampling);
   frames accumulate in a bounded in-memory buffer (default 1,000,000 frames
   per edge process). On restore, the buffer drains in seq order ahead of
   live traffic, throttled by confirms. If the cap is hit, the **oldest**
   frames drop first and a `buffer_dropped` counter and gap event record the
   loss honestly. Drop-oldest is a deliberate choice: fresh data serves the
   real-time consumer better than stale data, and the gap machinery already
   accounts for holes; the alternative (drop-newest) preserves contiguity but
   starves the live view. A production edge would spill to disk; the decision
   log records why the lab does not.
3. **Broker → ingest (classic)**: durable queue, manual ack, prefetch
   `CB_PREFETCH` (default 512). Ack happens **only after** the database
   commit that contains the frame. Consumer death requeues unacked frames
   (redelivery), which the sink absorbs. Unparseable or schema-invalid frames
   are rejected without requeue and dead-letter to `cb.frames.dlq`: poison
   input must never wedge the pipeline.
4. **Broker → ingest (stream, phase 3)**: offset-tracked consumer via the
   native stream protocol. The offset is stored server-side under consumer
   name `cb-ingest` **only after** the database commit. Crash and restart
   replays the tail since the last stored offset; the sink absorbs the
   replay. Reads are non-destructive, so replay from any offset or timestamp
   is a first-class demo.
5. **Ingest → TimescaleDB**: frames buffer in memory and flush as one
   multi-row `INSERT ... SELECT unnest(...) ON CONFLICT DO NOTHING` when the
   batch reaches `CB_BATCH_MAX_FRAMES` (default 500) or
   `CB_BATCH_MAX_MS` (default 200) elapses, whichever first. The conflict
   target is the sink's uniqueness key `(pad_id, well_id, epoch, seq,
   time)`;
   conflicts are counted as `dup_dropped` and are the observable measure of
   duplicate absorption. Ack (or offset store) follows commit; a crash
   between commit and ack yields redelivery, absorbed as above.

**Ordering.** Global order is not promised and not needed; the promise is
per-`(pad, well)` order **at rest**, reconstructed by `seq`. In classic mode
a single consumer sees FIFO order except after redelivery; competing
consumers interleave freely. The sink is therefore order-independent: any
arrival order lands correctly, and reads sort by `(time, seq)`. In stream
mode the stream itself is totally ordered and offset-monotonic. This
distinction (ordering scope, and moving the ordering burden from transport to
data model) is a core lesson the project exists to demonstrate.

**Gap tracking.** Ingest keeps, per `(pad, well)`: the highest contiguous
seq and a bounded set of open ranges. A frame beyond `expected` opens a gap
and emits a `gap_opened` event; arrivals inside an open range shrink it; a
range emptied emits `gap_healed` with the heal latency (store-and-forward
drains surface here as heals, not losses). Open-range memory is bounded;
overflow collapses to a summary counter rather than growing without limit.

## 6. Persistence

Database `coldbore` on TimescaleDB (PostgreSQL 17). Schema highlights (full
DDL lives in `infra/timescale/init/`):

```sql
CREATE TABLE frames (
  time         TIMESTAMPTZ NOT NULL,   -- event time (frame t_ms)
  pad_id       SMALLINT    NOT NULL,
  well_id      SMALLINT    NOT NULL,
  epoch        BIGINT      NOT NULL,    -- producer generation
  seq          BIGINT      NOT NULL,
  pressure_psi REAL        NOT NULL,
  rate_bpm     REAL        NOT NULL,
  proppant_ppa REAL        NOT NULL,
  temp_f       REAL        NOT NULL,
  inserted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (pad_id, well_id, epoch, seq, time)
);
SELECT create_hypertable('frames', 'time', chunk_time_interval => INTERVAL '15 minutes');
```

- The unique index is the idempotency backstop; `time` participates because a
  hypertable's unique constraints must include the partitioning column (and a
  duplicate frame carries the identical `time`, so the constraint still
  fires).
- At startup the ingest seeds its gap accounting from the durable store
  (newest epoch + max seq per well), so a restarted consumer never reports
  already-landed history as open gaps.
- A continuous aggregate `frames_1s` (1-second per-well OHLC-style rollup)
  backs dashboard history queries so they never scan raw frames.
- Compression policy on chunks older than 1 hour: the compression ratio is a
  reportable number.
- `events` (faults, gaps, heals, lifecycle; plain table), `service_metrics`
  (1 Hz snapshots as a small hypertable), and `scenario_runs` (phase 4)
  complete the schema.
- Completeness is computable purely from SQL: expected seq span vs `count(*)`
  per well over a window. The scoring engine uses exactly this.

## 7. Control plane

JSON commands published by the api to `cb.control.x` (fanout); each service
applies commands addressed to it and reports via a `fault_applied` event:

| `cmd` | Args | Target | Effect |
|---|---|---|---|
| `link` | `pad`, `state: up\|down` | edge | sever/restore one pad's uplink (store-and-forward) |
| `dup` | `rate: 0.0..1.0` | edge | re-publish that fraction of confirmed frames |
| `reorder` | `window: u32` (0 = off) | edge | emit frames in shuffled windows of that size |
| `rate` | `multiplier: f64` | edge | scale generation frequency (volume surge) |
| `kill` | `service: ingest\|edge` | named | process exits non-zero; supervisor restarts it |
| `reset` | | all | clear all injected faults to defaults |

Faults live **only** in the edge's publish path and the process supervisor;
the ingest data path has no test-only branches. What the consumer survives,
it survives for real.

## 8. Telemetry plane

1 Hz snapshot per service to `metrics.{service}`; discrete events to
`events.{kind}`. The api persists events, folds broker stats from the
RabbitMQ management API (queue depth, unacked, publish/deliver rates) into
the snapshot stream, and fans everything out over WebSocket.

Edge snapshot: `generated`, `published`, `confirmed`, `retransmits`,
`buffered`, `buffer_dropped`, `dup_injected`, `rate_hz`, per-pad link state.
Ingest snapshot: `consumed`, `inserted`, `dup_dropped`, `poison`,
`redeliveries`, `batches`, `open_gaps`, `healed`, e2e latency `p50/p95/p99`
(per-second histogram over frames flushed that second), `mode`, and in stream
mode the committed offset.

## 9. SLOs and measurement definitions

| SLO | Definition | Default target |
|---|---|---|
| Completeness | distinct `(pad, well, seq)` landed / seqs generated, per window | 100% |
| End-to-end latency | `inserted_at - to_timestamp(t_ms)` per frame; p99 over 1 s windows | p99 < 1.5 s |
| Consumer lag | classic: queue depth + unacked (mgmt API); stream: tail offset - committed offset, and seconds-behind derived from tail timestamp | drains to < 1 s of backlog |
| Recovery time | fault-cleared event → first snapshot with lag under target | scenario-defined |

Latency honesty: batching adds up to `CB_BATCH_MAX_MS` to every frame by
design; the baseline write-up states the batching contribution separately.
Event-time vs wall-clock skew is negligible on one host and called out as a
thing a multi-host deployment must solve (NTP/PTP discipline).

## 10. Failure drill matrix

Each drill maps to an interview-grade question and has an expected signature:

| Drill | Injection | Expected signature |
|---|---|---|
| Field link loss | `link down` on pad N | edge `buffered` climbs; DB shows a growing per-well gap; on restore, drain spike, `gap_healed`, completeness returns to 100% |
| Duplicate delivery | `dup 0.05` | ingest `dup_dropped` ≈ 5% of throughput; completeness unchanged; DB row count unaffected |
| Out-of-order arrival | `reorder 64` | no sink errors; gaps open and heal within milliseconds; ordering at rest intact |
| Consumer death | `kill ingest` | queue depth climbs; supervisor restarts; redeliveries absorbed; lag drains; zero loss |
| Volume surge | `rate 20` | throughput ceiling becomes visible: confirm RTT, batch sizes, queue depth, p99 all move; where it saturates is the finding |
| Poison input | malformed frame via api debug endpoint | frame lands in `cb.frames.dlq`; pipeline unaffected |
| Broker restart | `docker compose restart rabbitmq` | edge store-and-forward covers the outage; both durable queue and stream retain; recovery measured |

## 11. Queues → Streams migration (phase 3)

The motivating scenario: transaction volume grows and destructive,
per-message-acked queues become the bottleneck and the operational risk. The
migration lab keeps the workload identical and swaps the transport:

- Same producer, same frames; publish via the native stream protocol with
  producer-name deduplication (publishing id = seq) as a second dedup layer.
- Consumer moves from ack/prefetch to offset tracking; the "what did we
  commit" question moves from the broker's unacked ledger to an explicit
  offset the consumer owns. Single-active-consumer covers the failover
  story.
- Replay becomes free (non-destructive reads): `--from first|offset:N|ts:T`
  re-materializes the database from the stream, which is also the disaster
  recovery drill.
- Retention becomes a size/time policy, not "consumed = gone".

Deliverable: `docs/benchmarks/` write-up with the same drill matrix and load
curve run in both modes: throughput ceiling, p99 under surge, recovery time
after consumer death, and a semantics-diff table (what each mode promises,
what each one made us handle ourselves).

## 12. Game layer (phase 4)

Scenarios are YAML files under `scenarios/`: a timeline of control commands,
SLO objectives, and scoring weights. The engine (api) executes the timeline,
computes scores from SQL over `frames`/`events`/`service_metrics`, and the
dashboard presents it as a shift: alarms fire, the player acts (or a scripted
"autopilot" plays the incident), the score is data integrity, recovery time,
and latency held. Every scenario is a named, repeatable experiment; the game
skin is thin by design and cut first if time pressure demands.

## 13. Phasing and spec map

| Phase | Delivers | Specs |
|---|---|---|
| 0 | Governance, this document, CI gates | 000, 001 |
| 1 | Pipeline straight through in classic mode, baseline numbers | 002 proto, 003 infra, 004 edge, 005 ingest, 006 api, 007 dashboard |
| 2 | Fault injection, gap machinery, live SLO dashboard | amendments to 004/005/006/007 |
| 3 | Streams mode, replay, A/B write-up | 008 streams migration |
| 4 | Scenario engine, scoring, polish | 009 scenarios |

Every phase ends with the working tree green through the gate chain
(`compile → index → lint → couple`) and, from phase 1 on, a measured entry
under `docs/benchmarks/`.

## 14. Decision log

- **JSON frame encoding.** Chosen for cross-language legibility, dashboard
  inspectability, and interview demoability. Cost: ~3-5x the bytes and CPU of
  a fixed binary layout. The ceiling this imposes is itself a measured,
  discussable result; a `postcard`/protobuf encoding is the known next step
  and the proto crate isolates the change.
- **One queue, not per-pad queues.** A single `cb.frames.q` makes the
  ordering-scope lesson visible (competing consumers interleave) and keeps
  the lab simple. Per-pad queues (or stream partitioning) are the documented
  scaling step.
- **Bounded in-memory store-and-forward, drop-oldest.** See §5.2. Disk
  spill is a production necessity but adds recovery machinery (segment
  files, fsync policy, restart scan) orthogonal to what the lab teaches.
- **Metrics over the broker, not Prometheus.** See §1.
- **Docker Compose, not Kubernetes.** One laptop, one broker, five
  processes; compose keeps the demo one command. The role treats k8s as
  supporting skill; the write-up notes what a k8s deployment changes
  (supervisor becomes a liveness probe, `kill` becomes pod deletion).
- **Python for the api.** Matches the modeled platform's split (Rust on the
  hot path, Python for services/reporting) and exercises asyncpg + aio-pika,
  the libraries this API layer would really use.
