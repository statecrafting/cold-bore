# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## What this is

cold-bore is an interactive, fault-injectable scale model of a real-time
completions-style data platform: simulated frac pads → edge store-and-forward
→ RabbitMQ (classic queues and Streams) → Rust ingest → TimescaleDB → FastAPI
REST/WebSocket → browser dashboard, with a control plane for fault injection
and a scenario/scoring ("game") layer on top. Read
`docs/design/architecture.md` first: it is the load-bearing design doc
(components, broker topology, the hop-by-hop delivery contract, schema,
control/telemetry vocabularies, failure drill matrix, streams migration plan,
phasing, decision log).

The repo is governed by **spec-spine** (installed binary on PATH; CI installs
`spec-spine-cli` from crates.io). Every component is owned by a spec under
`specs/`; the gate chain is `compile → index → lint → couple`.

## Commands

```sh
# Infrastructure (RabbitMQ 4 + TimescaleDB)
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml down -v   # destroys data

# Rust (workspace: coldbore-proto, coldbore-edge, coldbore-ingest)
cargo build --workspace --locked
cargo test  --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo run -p coldbore-edge     # needs infra up; env: CB_* (see proto::config)
cargo run -p coldbore-ingest

# Python API (uv-managed, services/api)
cd services/api && uv sync
cd services/api && uv run uvicorn app.main:app --port 8000
cd services/api && uv run pytest
cd services/api && uv run ruff check .

# Governance gates (run before every PR; CI runs the same chain)
spec-spine compile && git diff --exit-code .derived/spec-registry
spec-spine index && git diff --exit-code .derived/codebase-index
spec-spine index check          # staleness gate (exit 2 if stale)
spec-spine lint --fail-on-warn
spec-spine couple --base origin/main --head HEAD
```

The installed spec-spine (0.10.x) has no `compile --check`; registry
freshness is checked by recompiling and diffing (compile is deterministic, so
this is sound). When a release ships `compile --check`, switch to it.

## Invariants (do not violate without updating the design doc and owning spec)

**Delivery guarantees are the product.** The pipeline is at-least-once end to
end, effectively exactly-once at the sink:

- `seq` is assigned only by the edge, monotonic per `(pad, well)`, never
  reused. It is the idempotency and gap key for the whole system.
- Never ack (classic) or store an offset (stream) before the database commit
  containing that frame has returned.
- The sink is order-independent and idempotent: `ON CONFLICT (pad_id,
  well_id, seq, time) DO NOTHING`, conflicts counted as `dup_dropped`.
- Poison input dead-letters to `cb.frames.dlq`; it never wedges or crashes
  the consume loop.
- Fault injection lives only in the edge publish path and the process
  supervisor. The ingest data path has no test-only branches: what the
  consumer survives, it survives for real.
- Bounded buffers everywhere; every drop is counted and surfaced as a metric
  and gap event. Silent loss is the one unforgivable bug.

**Cross-language contracts are spec-governed.** Frame, control-command,
metrics-snapshot, and event JSON shapes are shared by Rust and Python and
documented in the architecture doc; changing one means changing the owning
spec, both implementations, and the doc in the same PR.

**Numbers discipline.** Every phase from 1 on lands a measured entry under
`docs/benchmarks/` (method, environment, numbers, interpretation). A
performance claim without a benchmark entry does not merge.

## Self-governance workflow

- `.derived/spec-registry/by-spec/` and `.derived/codebase-index/{by-spec,by-package}/`
  shard trees are **committed** (only `build-meta.json` is gitignored). After
  any change affecting them: `spec-spine compile && spec-spine index`, commit
  the shards. CI fails if they are stale.
- Spec-first: a change to code owned by a spec carries the spec edit in the
  same PR, or a `Spec-Drift-Waiver:` line in the PR body. New capabilities
  get the next `NNN-slug` directory under `specs/`.
- If the coupling gate fails because code and its owning spec disagree, do
  not edit the spec to match freshly written code; surface the contradiction
  (see `.claude/rules/adversarial-prompt-refusal.md`).
- Read derived artifacts only through `spec-spine` subcommands, never ad-hoc
  JSON parsing (see `.claude/rules/governed-artifact-reads.md`).
