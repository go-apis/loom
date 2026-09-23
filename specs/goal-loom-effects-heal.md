---
title: Goal — An effect settles even when its context died, and retries over minutes
kind: plan
status: draft
---

# Goal — An effect settles even when its context died, and retries over minutes

Mesh goal `loom-effects-heal`, running.

## Statement

G2: settle an effect's outcome on context.WithoutCancel so a call that failed after its deadline is recorded failed, not left in doubt; `@retry(max, min..max)` per process or effect as durable timers over the dead letter; `@idempotent` effects and an optional Reconcile hook consulted before declaring doubt; resolve with redrive: true in one call. The brief is runsheet/runsheet specs/proposal-loom-for-adr-001.md (Prologue, project runsheet), section 3. loom has ten consumers (runsheet, wardroom, latchkey, grapevine, optrader, ten99 on v0.49.0; tripline, purser, foghorn on v0.46.0): every change is backward compatible or ships behind an opt-in, and the consumers' adoption is their own goals, not this one's.

## Acceptance

Tests: a reaction whose ctx is cancelled mid-call is marked failed, not running; an effect failing with a transient error is retried over five minutes and lands with no dead letter; a crash after the call and before settle converges without a person on an idempotent or reconciled effect.

## Items

| Seq | Title | Status | Branch | PR | Cost |
| --- | --- | --- | --- | --- | --- |
| 1 | Settle an effect's outcome on a context that outlives cancellation | claimed | — | — | $0.00 |
| 2 | @idempotent effects and a Reconcile hook resolve doubt without a person; resolve takes redrive: true | pending | — | — | $0.00 |
| 3 | @retry(max, min..max): exhausted reactions re-arm as durable timers over the dead letter | pending | — | — | $0.00 |

## Log

- 2026-09-23 · item 1 · session started
