---
id: "003-infra"
title: "Infrastructure: broker, database, provisioning"
status: approved
created: "2026-08-27"
summary: >
  The docker-compose lab: RabbitMQ 4 (management + stream plugins, localhost
  port bindings, stream advertised host) and TimescaleDB (PostgreSQL 17) with
  the schema migrations that define the frames hypertable, its identity
  index, the frames_1s continuous aggregate, compression policy, and the
  events / service_metrics tables. Plus the run scripts that supervise the
  services during drills.
establishes:
  - "infra/"
  - "scripts/"
depends_on:
  - "001-architecture"
---

# 003: Infrastructure

## 1. Purpose

One `docker compose up` yields the whole substrate; everything else is
`cargo run` and `uvicorn`. The schema is the durable half of the delivery
contract: the identity index is what makes the sink idempotent.

## 2. Territory

`infra/docker-compose.yml`, `infra/rabbitmq/` (enabled_plugins,
rabbitmq.conf), `infra/timescale/init/*.sql`, `scripts/run-*.sh`.

## 3. Behavior

- RabbitMQ 4.1 with `rabbitmq_management`, `rabbitmq_stream`,
  `rabbitmq_stream_management`; AMQP 5672, management 15672, stream 5552,
  all bound to 127.0.0.1. `stream.advertised_host = localhost` so native
  stream clients on the host survive the post-handshake redirect.
- Credentials `coldbore:coldbore` (a lab; not a deployment pattern).
- TimescaleDB on host port 5433 (avoiding a local postgres). Migrations run
  from `docker-entrypoint-initdb.d` on first boot; `down -v` resets.
- `frames` hypertable: 15-minute chunks; unique index
  `(pad_id, well_id, epoch, seq, time)` (the idempotency backstop; `epoch`
  makes seq reuse across producer restarts non-colliding; `time`
  participates because hypertable unique indexes must include the partition
  column). Compression after 1 hour, segmented by `(pad_id, well_id)`.
- `frames_1s` continuous aggregate refreshes every 2 s and backs all
  dashboard history reads; raw `frames` is never scanned for charts.
- `events` and `service_metrics` capture the telemetry plane;
  `service_metrics` is itself a hypertable.
- `scenario_runs` (spec 009): scenario executions and their score
  breakdowns (`002_scenarios.sql`; apply manually on an existing volume).
- `stream_offsets` (spec 008): the stream consumer's committed offset,
  updated in the same transaction as the batch it covers; the broker-side
  offset store is observability, this row is the truth.
- Run scripts supervise: a service that exits non-zero (the `kill` drill)
  restarts after one second, so crash drills measure recovery, not
  babysitting. Supervision only works because the services fail fast
  (specs 004/005): a process that hangs instead of exiting is outside any
  supervisor's reach.
- `scripts/run-all.sh` lights the whole substrate in one command: infra up
  (waiting on container health), then the three supervised services in the
  background (logs and pidfiles under `.run/`, gitignored).
  `scripts/stop-all.sh` stops the services (and infra with `--infra`).

## 4. Out of scope

Kubernetes (decision log §14); containerizing the services themselves
(compose runs infra only in v1); the stream declaration itself (phase 3
amends this spec if broker config changes).
