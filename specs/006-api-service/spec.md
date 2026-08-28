---
id: "006-api-service"
title: "API service: egress, control entry point, telemetry persistence"
status: approved
created: "2026-08-27"
summary: >
  The Python egress layer: FastAPI REST + WebSocket fan-out of the telemetry
  plane, the RabbitMQ management poller (broker-side lag truth), validated
  control-command publishing, persistence of events and metric snapshots,
  and the read queries over the continuous aggregate. Loses nothing upstream
  when it dies.
establishes:
  - "services/api/"
depends_on:
  - "001-architecture"
  - "002-telemetry-model"
  - "003-infra"
---

# 006: API service

## 1. Purpose

The customer-facing edge of the model platform, and the operator's hands:
what the dashboard sees, it sees through this service; what the operator
does, this service publishes to the control plane.

## 2. Territory

`services/api/`: `app/broker.py` (telemetry consumer, control publisher),
`app/mgmt.py` (management API poller), `app/db.py` (asyncpg persistence and
reads), `app/ws.py` (WebSocket hub), `app/control.py` (the Python mirror of
the command contract), `app/main.py` (lifespan, routes), `tests/`.

## 3. Behavior

- **A consumer like any other.** The api binds its own queue on the
  telemetry exchange; its death loses nothing upstream and its restart
  resumes cleanly. Startup tolerates missing infra (retry loops), and the
  app serves whatever planes are alive.
- **Control commands are validated twice**: pydantic (discriminated union
  mirroring spec 002's serde shape, same bounds) before publish; services
  re-validate on receipt. Invalid commands never reach the exchange.
- **Broker-side lag truth**: queue depth, unacked, publish/deliver rates
  polled from the management API at 1 Hz; in classic mode this is the
  authoritative lag signal (a consumer cannot see undelivered backlog).
- **WebSocket slow-consumer policy**: per-client bounded queue (500), drop
  oldest on overflow, drops counted and exposed in /api/status. A stalled
  tab degrades its own feed, never the hub.
- **Persistence**: events and metric snapshots land in their tables as they
  arrive; history reads go to `frames_1s`, never raw frames.
- **Stream awareness** (spec 008 amendment): the broker poll includes the
  stream's stats (retained records, consumers) under a `stream` key
  alongside the classic queue's.
- REST surface: `/api/status`, `/api/history`, `/api/wells`,
  `/api/completeness` (seq-span vs rows per well), `/api/events`,
  `POST /api/control`, `WS /ws`; the dashboard is served statically at `/`.

- **Scenario engine** (spec 009 amendment): `app/scenarios.py` runs one
  YAML scenario at a time (timeline of validated control commands, ending
  in `reset` + settle), scores it exclusively from the database, persists
  runs, and emits `scenario_*` events; routes `/api/scenarios`,
  `/api/scenarios/{id}/start`, `/api/runs`, plus the broker-side poison
  injector `/api/debug/poison`.
- **Substrate preflight.** The engine refuses to arm a scenario unless the
  substrate is pulsing: database connected, and both edge and ingest have
  reported a metrics snapshot within the last 5 s. The refusal (HTTP 409)
  names every problem; `/api/status` exposes the same list as
  `substrate_problems`. A scenario scored over a dead pipeline would be an
  F that says nothing, and the engine must not produce it.

## 4. Out of scope

Authentication (a localhost lab); Kafka egress (out of v1 scope
entirely).
