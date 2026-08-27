---
id: "007-dashboard"
title: "Dashboard: the night-shift console"
status: approved
created: "2026-08-27"
summary: >
  The no-build-step browser console: live charts (throughput, backlog,
  latency, absorption) fed over WebSocket with rates derived from cumulative
  counters, stat tiles, the fault console (link/dup/reorder/rate/kill/reset),
  the event log, and the per-well live grid. Dark-only ops aesthetic; the
  validated reference palette's dark steps.
establishes:
  - "dashboard/"
depends_on:
  - "001-architecture"
  - "006-api-service"
---

# 007: Dashboard

## 1. Purpose

Make the pipeline's behavior legible in real time: every drill in the
architecture doc §10 has a visible signature here, and every fault is one
click.

## 2. Territory

`dashboard/`: `index.html`, `style.css`, `app.js` (WS client, rate
derivation, fault console, wells grid, event log), `charts.js` (rolling
canvas line charts with crosshair + tooltip hover).

## 3. Behavior

- Plain ES modules, zero dependencies, zero build step; served statically
  by the api.
- Rates are derived client-side by differencing cumulative counters across
  snapshots; edge and ingest cadences merge into persistent series so lines
  never break between services' ticks.
- Charts: single y-axis each, 2 px lines, hairline grid, legend always
  present with live values, crosshair tooltip; series colors follow the
  reference palette's dark categorical order (blue, orange, aqua).
- Status colors are reserved for state (gap open, poison, drops) and always
  paired with a textual label, never color alone.
- The fault console posts spec-002 commands to `/api/control` verbatim; pad
  link state renders from edge metrics (the truth), not from local button
  state.
- The backlog series is transport-aware (spec 008 amendment): queue depth
  and unacked in classic mode; `retained - 1 - committed offset` in stream
  mode (mgmt chunk-lag caveat documented), with the edge buffer series
  common to both.
- WebSocket reconnects with capped backoff; the connection badge always
  states the truth.

- **Scenario console** (spec 009 amendment): scenario cards with run
  buttons, a live-run banner (countdown, steps fired), the score card
  (grade, component breakdown, detail line) driven by `scenario_scored`
  events, and the poison injector button.

## 4. Out of scope

Historical chart backfill beyond the live window; mobile layouts beyond
basic reflow; scenario editing UI.
