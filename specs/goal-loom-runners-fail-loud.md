---
title: Goal — A runner that cannot go on says so, and the rest go on
kind: plan
status: draft
---

# Goal — A runner that cannot go on says so, and the rest go on

Mesh goal `loom-runners-fail-loud`, running.

## Statement

G1: projection failure state on the checkpoint row, with backoff and a skip endpoint; lazy decode so a runner decodes only the events it subscribes to, an `@retired` event marker, and Migrate refusing unknown types in the log; an explicit start position for processes with from-head the default for a new one; ERROR on a never-checkpointed runner and failures logged at most once a minute; loom.CloudLogHandler writing `severity` so Cloud Logging's severity>=ERROR finds `runner step failed`. The brief is runsheet/runsheet specs/proposal-loom-for-adr-001.md (Prologue, project runsheet), section 3. loom has ten consumers (runsheet, wardroom, latchkey, grapevine, optrader, ten99 on v0.49.0; tripline, purser, foghorn on v0.46.0): every change is backward compatible or ships behind an opt-in, and the consumers' adoption is their own goals, not this one's.

## Acceptance

Tests: a fold that always fails leaves the runner stalled with the seq and error while other runners advance; a log holding an undeclared type fails Migrate and not the runners; a new process with effects performs none for events before it existed; the handler's JSON carries severity=ERROR for a failed step.

## Items

| Seq | Title | Status | Branch | PR | Cost |
| --- | --- | --- | --- | --- | --- |
| 1 | Lazy decode, @retired events, and Migrate guards against undeclared log types | running | — | — | $0.00 |
| 2 | Projection runners fail loud: stalled state, backoff, and a skip endpoint | pending | — | — | $0.00 |
| 3 | Explicit process start position, @from(head) default for a new process | running | — | — | $0.00 |
| 4 | loom.CloudLogHandler — severity for Cloud Logging | claimed | — | — | $0.00 |

## Log

- 2026-09-23 · item 4 · session started
