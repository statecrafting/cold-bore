---
name: setup
description: One-time contributor setup. Install toolchain prerequisites, bring up infra, and verify the governed loop (compile, index check, lint, couple) so `/init` can report lifecycle and structural counts.
allowed-tools: Bash, Read
---

# Setup

Get a fresh clone operational. After this completes, `/init` can report
lifecycle and structural counts through the installed `spec-spine` binary
(no ad-hoc parsing of `.derived/**/*.json`: see
`.claude/rules/governed-artifact-reads.md`), and the pipeline can run
locally.

## Process

### 1. Prerequisites

```bash
spec-spine --version || cargo install spec-spine-cli --locked
cargo --version          # Rust toolchain (stable)
docker compose version   # for RabbitMQ + TimescaleDB
uv --version             # for services/api
```

Halt on a missing prerequisite and surface which one.

### 2. Verify the governed loop

```bash
spec-spine compile && git status --porcelain .derived/spec-registry
spec-spine index check      # codebase index staleness gate (exit 2 = stale)
spec-spine lint             # corpus conformance
spec-spine couple           # PR-time coupling gate (vs origin/main)
```

A non-empty `git status` after `compile` means the committed registry is
stale: commit the recompiled shards (or `git checkout -- .derived/` if the
corpus itself is unchanged and this was a version-skew artifact). If
`index check` exits non-zero, regenerate with `spec-spine index` and commit.
Do not parse `.derived/**/*.json` directly to "verify" success.

### 3. Build the workspace (phase 1+)

```bash
test -f Cargo.toml && cargo build --workspace --locked
test -f services/api/pyproject.toml && (cd services/api && uv sync)
```

### 4. Emit summary

Report exactly:

```
## setup: cold-bore

**Prerequisites:** {ok / missing <tool>}
**Governed loop:**
  - compile: {fresh registry / stale shards listed}
  - index check: {fresh / stale}
  - lint: {clean / N diagnostics}
  - couple: {clean / drift surfaced}
**Build:** {ok / skipped (pre-phase-1) / failed at <step>}
**Lifecycle:** {N specs across <statuses>}  (from registry status-report)

Next: run `/init` to load full session context.
```

Do not invent counts. Only report values that came back from a
`spec-spine` subcommand.

## Rules

- The loop runs through the installed `spec-spine` binary (crates.io
  `spec-spine-cli`), never `npx spec-spine`.
- Halt on first failure. Do not silently continue past a missing
  prerequisite or a failing gate.
- Never parse `.derived/**/*.json` directly in any verification step.
  Use the `spec-spine` subcommands.
- Idempotent: safe to re-run. `compile` and `index` are deterministic.
