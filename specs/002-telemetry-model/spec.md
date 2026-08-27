---
id: "002-telemetry-model"
title: "Telemetry model and cross-language contracts"
status: approved
created: "2026-08-27"
summary: >
  The shared vocabulary of the pipeline: the sensor frame (with seq as the
  pipeline-wide idempotency and gap key), the control-command grammar, the
  metrics-snapshot and event shapes, broker object names, and the CB_* env
  configuration. Owned by the coldbore-proto crate; mirrored in Python by
  services/api.
establishes:
  - "crates/coldbore-proto/"
depends_on:
  - "001-architecture"
---

# 002: Telemetry model and cross-language contracts

## 1. Purpose

One crate owns every wire shape so that the engine (Rust) and the egress
(Python) cannot drift apart silently. The JSON forms in the architecture doc
§4, §7, §8 are normative; this crate is their Rust source of truth.

## 2. Territory

`crates/coldbore-proto/`: `frame` (Frame, validation, routing key, message
id), `control` (internally-tagged command enum + bounds validation),
`metrics` (EdgeMetrics, IngestMetrics, LatencyPercentiles, Event, the event
kind vocabulary), `topology` (broker object names), `config` (CB_* env
parsing, panic-free).

## 3. Behavior

- `Frame.epoch` is the producer generation (edge process start, ms);
  `Frame.seq` is assigned only by the edge, monotonic per `(pad, well)`
  within an epoch, starting at 1, never reused. Every downstream mechanism
  (idempotent sink, gap tracking, completeness scoring) keys on
  `(pad, well, epoch, seq)`; a restarted producer can therefore never
  collide with or masquerade as its previous generation.
- Frame validation rejects unknown versions and non-finite channel values;
  consumers treat validation failure as poison, never as a crash.
- Counters in metric snapshots are cumulative since process start; consumers
  derive rates by differencing. A missed snapshot is harmless.
- Control commands carry their own bounds (`dup` in [0,1], `reorder` <=
  4096, `rate` in (0,100]); both the api (pre-publish) and services
  (post-receive) validate.
- The Python mirror of these contracts lives in `services/api/app/control.py`
  (commands) and in the api's tolerant reading of snapshots/events. A change
  to any wire shape changes this crate, the Python mirror, the architecture
  doc, and this spec in the same PR.

## 4. Out of scope

Transport (how frames move: 004/005), persistence shapes (003), scenario
grammar (phase 4).
