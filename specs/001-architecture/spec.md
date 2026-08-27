---
id: "001-architecture"
title: "System architecture and delivery contract"
status: approved
created: "2026-08-27"
summary: >
  The load-bearing shape of cold-bore: a fault-injectable scale model of a
  completions-style data platform (edge simulator, RabbitMQ data/control/
  telemetry planes, Rust ingest, TimescaleDB, Python API/WebSocket egress,
  dashboard game console). Owns the architecture document and binds every
  component spec to the hop-by-hop delivery contract defined there.
establishes:
  - "docs/design/architecture.md"
depends_on:
  - "000-bootstrap"
---

# 001: System architecture and delivery contract

## 1. Purpose

Fix the system shape and the non-negotiable delivery semantics before any
component exists, so that component specs (002+) refine a stated whole
rather than accreting one.

## 2. Territory

This spec owns `docs/design/architecture.md`. The architecture document is
normative for:

- the three-plane broker topology (data, control, telemetry) and the object
  names in its §3 (the stream's dual binding is spec 008's amendment);
- the frame contract (§4), including that `seq` is assigned only by the
  edge, monotonic per `(pad, well)` within a producer generation, and that
  pipeline-wide identity (idempotency and gap key) is
  `(pad, well, epoch, seq)`, where `epoch` is the producer generation: a
  restarted producer can never collide with or masquerade as its previous
  generation;
- the hop-by-hop delivery contract (§5): at-least-once end to end,
  effectively exactly-once at the sink; ack/offset-store strictly after
  database commit; order-independent idempotent sink; dead-lettered poison;
  bounded buffers with counted, evented drops;
- the SLO measurement definitions (§9);
- the phasing and spec map (§13).

## 3. Behavior

Component specs MUST declare `depends_on: ["001-architecture"]` (directly or
transitively) and MUST NOT weaken the delivery contract. A change that
alters any normative section of the architecture document amends this spec
in the same change.

## 4. Out of scope

Component internals (each component spec owns its own crate or service
directory); benchmark results (recorded under `docs/benchmarks/`, which is
documentation, not authority).
