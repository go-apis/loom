---
title: Goal — loom has CI, and a merge makes a tag
kind: plan
status: draft
---

# Goal — loom has CI, and a merge makes a tag

Mesh goal `loom-release-path`, running.

## Statement

G0, loom's half. loom has no workflows: add CI running go test ./... with a Postgres service container, and tag on merge — every merge to main gets the next minor tag unless it carries a patch label. The bump PRs on consumers are runsheet's half, a separate goal. The brief is runsheet/runsheet specs/proposal-loom-for-adr-001.md (Prologue, project runsheet), section 3. loom has ten consumers (runsheet, wardroom, latchkey, grapevine, optrader, ten99 on v0.49.0; tripline, purser, foghorn on v0.46.0): every change is backward compatible or ships behind an opt-in, and the consumers' adoption is their own goals, not this one's.

## Acceptance

A PR's CI runs the full suite against Postgres and blocks the merge when red; merging a one-line PR produces the next tag without a person; a patch-labelled merge produces a patch tag.

## Items

| Seq | Title | Status | Branch | PR | Cost |
| --- | --- | --- | --- | --- | --- |
| 1 | CI: go test ./... against a Postgres service container | claimed | — | — | $0.00 |
| 2 | Tag on merge: next minor unless patch-labelled, plus the release-path ADR | pending | — | — | $0.00 |

## Log

- 2026-09-23 · item 1 · session started
