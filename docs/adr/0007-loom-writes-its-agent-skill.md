---
title: "ADR 0007 — loom writes its agent skill into the consumer, stamped with its version"
status: accepted
date: 2026-10-18
summary: `loom skill` (and `loom init`) writes a Claude Code skill at the repository root's `.claude/skills/loom/SKILL.md`, rendered from a template embedded in the loom module and stamped with the loom version that wrote it; `loom skill --check` fails when it is missing or stale, so the advice an agent reads matches the generator the repository pins.
---

# ADR 0007 — loom writes its agent skill into the consumer, stamped with its version

## Context

Most of the code in loom services is now written by agents, and the
traps they hit are loom's, not the service's: on 2026-10-18 alone,
runsheet's sessions regenerated loomgen with an unpinned generator (main
stopped compiling for 2.5 hours), named a process after its aggregate
(ADR 0006), wrapped a call in `loom.Once` under an undeclared effect (the
reaction parked to dead letters), and folded a list from an event that
carried only the new element. Each rule is in the README, which an agent
does not read before it edits a schema. A Claude Code skill is read when
the task matches its description, costs nothing otherwise, and lives in
the repository the session runs in — which is where Mesh's build sessions
run.

## Decision

- **The skill ships in the loom module** (`skill/files/SKILL.md`,
  embedded) and is rendered with the version of loom that writes it, in
  its frontmatter (`metadata.loom-version`) and its text.
- **`loom skill` writes it at the repository root**, found by walking up
  from the current directory to `.git` (or `--root`), at
  `.claude/skills/loom/SKILL.md` — the project skills directory every
  Claude Code session in that repository reads. It overwrites: the file is
  loom's, as loomgen is.
- **`loom init` writes it too**, unless the repository already carries one.
- **`loom skill --check`** exits non-zero, naming `go tool loom skill`,
  when the file is missing or differs from what this version writes — a
  hand edit or a loom bump. Run with the pinned generator (ADR 0005),
  `go tool loom skill --check` belongs beside `generate --check` in CI.

## Consequences

- A loom bump changes the skill's bytes (its version stamp at least), so
  a consumer that checks it regenerates it with the bump, as it does
  loomgen. The advice always matches the generator.
- A repository's own rules about loom (its grandfathered reactor names,
  its schema directory) stay in its AGENTS.md; the skill is loom's, and a
  consumer that edits it has it rewritten.
- Repositories without Claude Code lose nothing: the file is Markdown.
