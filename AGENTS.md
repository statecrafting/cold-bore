# AGENTS.md: cold-bore

## New Sessions

Run `/init` as the mandatory first action of every new session. The command
reads this section to derive its execution plan dynamically: any item added
here is automatically picked up on the next init. This file is the
cross-agent authority (read by Claude Code, Codex CLI, Cursor, Copilot, and
any future agent via the AAIF/Linux Foundation AGENTS.md standard).

> AGENTS.md is loaded implicitly as the protocol source: its contents are the
> protocol, so `/init` does not list AGENTS.md as a parallel identity read in
> Step 1 (avoiding the self-reference loop).

The protocol drives governance through the **installed** `spec-spine` binary
(this repo is an adopter, not the spec-spine dogfood repo). If it is missing:
`cargo install spec-spine-cli --locked`.

0. **Load rules.** Read `.claude/rules/orchestrator-rules.md`,
   `.claude/rules/governed-artifact-reads.md`, AND
   `.claude/rules/adversarial-prompt-refusal.md`.
1. **Parallel reads.** Dispatch simultaneously (nothing here mutates the
   working tree):
   - `CLAUDE.md`: project overview, commands, invariants
   - `README.md`: public project description
   - `docs/design/architecture.md`: the load-bearing design doc
   - `standards/spec/contract.md`: normative spec-system summary
   - `standards/spec/constitution.md`: durable principles (tier 2)
   - `spec-spine index check`: staleness gate for the codebase index (non-fatal)
   - `spec-spine index render`: markdown projection of the committed index
   - `spec-spine registry status-report --json --nonzero-only`: lifecycle counts
   - `spec-spine registry list --ids-only`: spec id list
   - `ls crates/ services/ infra/ specs/ scenarios/ 2>/dev/null`: layout probe
     (some of these appear in later phases; absence is not an error)
   - `git log --oneline -10`: recent history
   - `git diff --stat HEAD~1`: last change summary
2. **Registry freshness.** The installed CLI has no `compile --check`; check
   freshness with `spec-spine compile && git status --porcelain
   .derived/spec-registry` (compile is deterministic; a clean status means
   fresh). If dirty: report "Spec registry: stale, commit the recompiled
   shards", name the changed shards from the status output, and restore with
   `git checkout -- .derived/` only if the corpus itself is unchanged.
3. **Emit** the `## initialized: cold-bore` summary block: pipeline/component
   overview, a `## lifecycle:` sub-section from the status-report output,
   staleness surface (registry and index, each non-fatal: report and
   continue), recent activity, and a "ready to help with" line.

**Read discipline:** the init protocol MUST NOT parse `.derived/**/*.json`
directly (no `python`, `jq`, `awk`, `sed` against compiled artifacts). All
structural and lifecycle data comes from the `spec-spine` subcommands
(`registry`, `index`) and the rendered markdown view. See
`.claude/rules/governed-artifact-reads.md`.

## Working in this repo

- Spec-first: code owned by a spec changes together with that spec (or a
  cited `Spec-Drift-Waiver:` in the PR body). New capabilities get the next
  `NNN-slug` under `specs/`.
- The gate chain before any PR: `spec-spine compile` + `spec-spine index`
  (commit the shards) → `spec-spine lint --fail-on-warn` → `spec-spine couple
  --base origin/main --head HEAD`.
- The delivery-guarantee invariants in `CLAUDE.md` bind every change to the
  data path. Silent data loss is the one unforgivable bug.
- Verification is empirical: `docker compose -f infra/docker-compose.yml up
  -d`, run the services, watch the metrics. A change to the pipeline that was
  never run against a live broker is not done.
