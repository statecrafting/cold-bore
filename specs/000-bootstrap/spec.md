---
id: "000-bootstrap"
title: "Bootstrap the cold-bore spec system"
status: approved
created: "2026-08-27"
summary: >
  Foundational contract: authored truth lives only in markdown (+ YAML
  frontmatter); machine-consumable truth is compiler-emitted JSON only;
  every artifact is a deterministic function of (config, file contents);
  a typed authority graph governs who-owns-what.
origin:
  retroactive: true   # authority held since before the graph existed
unamendable:
  - "markdown-truth-boundary"
  - "json-truth-boundary"
  - "determinism-requirement"
  - "typed-authority-graph"
  - "refusal-rule"
---

# 000: Bootstrap the cold-bore spec system

This is the spec that defines what a spec *is* for the cold-bore repository.
Ordinary specs live under `specs/NNN-slug/spec.md`; each compilation unit
links back here (or to a more specific spec) via
`[package.metadata.spec-spine].spec` in its manifest, a `// Spec:` comment
header, or a spec's ownership edge.

cold-bore's corpus follows the phase plan in `docs/design/architecture.md`
§13: each phase files or amends the specs that own the code it lands, in the
same change.

## 1. The authoring / derived boundary

Humans author markdown; the compiler owns the JSON. Never hand-edit a
derived artifact. The `.derived/` shard trees are committed so the gates can
compare them against current inputs; only `build-meta.json` is ignored.

## 2. The typed authority graph

Specs declare typed edges (`establishes`, `extends`, `refines`,
`supersedes`, `amends`, `co_authority`, `constrains`, `references`) and
the units they own (file / section / symbol / directory / crate / module).
Authority is derived by walking the graph.

## 3. The refusal rule

Code owned by a spec does not change without that spec (or an explicit,
cited waiver). When code and spec disagree, the disagreement is surfaced,
never papered over by editing the spec to match freshly written code.
