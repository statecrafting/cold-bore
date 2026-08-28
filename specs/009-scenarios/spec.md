---
id: "009-scenarios"
title: "Scenarios and scoring: the game layer"
status: approved
created: "2026-08-27"
summary: >
  The night-shift game: YAML scenarios are timelines of control commands
  with SLO objectives and scoring weights. The api's engine fires the
  timeline, clears all faults, lets the pipeline settle, and scores the
  shift from SQL over what actually landed (completeness from seq spans,
  latency from metric snapshots, recovery from the last fault to the first
  in-SLO snapshot). Every scenario is a named, repeatable experiment.
establishes:
  - "scenarios/"
amends:
  - "006-api-service"
  - "007-dashboard"
  - "003-infra"
depends_on:
  - "001-architecture"
  - "002-telemetry-model"
---

# 009: Scenarios and scoring

## 1. Purpose

Give every failure drill a name, a repeatable timeline, and a number. The
game skin is thin by design: the score is computed exclusively from the
pipeline's own accounting, so a flattering score would require the
accounting to lie.

## 2. Territory

`scenarios/*.yaml` (the levels); the engine and its surface are amendments:
`services/api/app/scenarios.py` + routes (006), the dashboard scenario
panel and score card (007), the `scenario_runs` table (003).

## 3. The scenario grammar

- `id`, `title`, `tagline`, `duration_s`.
- `timeline`: `{at: seconds, cmd: <spec-002 control command>}` entries;
  every command is validated against the control contract at load time, so
  a corrupt scenario refuses to load rather than firing garbage.
- `objectives`: `completeness_pct`, `max_p99_ms`, `recovery_s`.
- `scoring`: weights per component (completeness / latency / recovery)
  summing to 100.

## 4. Behavior

- One scenario runs at a time (409 on overlap). The engine fires steps on
  the timeline, emits `scenario_*` events into the telemetry stream (so the
  event log and persistence see the shift the same way they see faults),
  always ends with `reset`, waits a fixed settle window, then scores.
- **Preflight before arming** (006 amendment): a scenario starts only over
  a pulsing substrate (database connected; edge and ingest snapshots
  within 5 s). Otherwise the start is refused with every problem named.
  An F must mean the pipeline failed the drill, never that nothing was
  running.
- Scoring reads only from the database: completeness = rows vs seq spans
  per (well, epoch) over the run (late fills count: that is what
  store-and-forward is for); latency = fraction of ingest snapshots with
  p99 inside the objective; recovery = time from the last timeline command
  to the first in-SLO snapshot (full credit within target, linear falloff
  to zero at 3x).
- Grades: S >= 95, A >= 85, B >= 70, C >= 50, else F. Runs persist in
  `scenario_runs` with the full score breakdown.
- A `kill` step relies on the supervised run scripts (spec 003): recovery
  is measured, not assumed.
- The debug poison injector (`POST /api/debug/poison`) publishes one
  malformed frame broker-side for the DLQ drill; it never touches the edge.

## 5. Out of scope

Multi-player or timed-input mechanics (the "player" acts through the same
fault console as always); scenario editing UI; difficulty progression.
