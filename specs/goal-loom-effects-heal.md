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
| 1 | Settle an effect's outcome on a context that outlives cancellation | running | — | — | $1.96 |
| 2 | @idempotent effects and a Reconcile hook resolve doubt without a person; resolve takes redrive: true | pending | — | — | $0.00 |
| 3 | @retry(max, min..max): exhausted reactions re-arm as durable timers over the dead letter | pending | — | — | $0.00 |

## Log

- 2026-09-23 · item 1 · session started
- 2026-09-23 · item 1 · Done — item 1 is committed on `mesh/loom-effects-heal-1` as `cacd059`. **What changed** - `effects.go:113` — `Once` now computes `settleCtx := context.WithoutCancel(ctx)` once fn has returned and passes it to both `settleEffect` calls (failure path and success path). `claimEffect` is untouched, as the brief specified — its claim commits before fn runs. - `effects.go:31-35` — package doc gains a paragraph: the settle write is decoupled from the reaction's own cancellation; only the call itself sees the reaction's ctx. - `DESIGN.md:177-181` — the Effects bullet now states the settle runs on `context.WithoutCancel` of the reaction's ctx, so a per-step deadline firing mid-call records `failed`/`done` instead of leaving the row in doubt. - `internal/e2e/effects_test.go` — new `TestEffectSettlesAfterContextCancelled`. It parks a dead letter by scripting the gateway to decline, deletes the effect row so the next attempt starts clean, then sets a 300ms gateway delay and calls `cli.RedriveDeadLetter` with a 100ms deadline — `RedriveDeadLetter` is the one exported path that runs a real reaction (effect scope and all) under a context the test controls. With a fresh uncancelled context it reads back via `cli.Effects` and asserts `status == "failed"` with a non-nil `settled_at` and the call's own error text. (I used 100ms/300ms rather than the brief's 15ms/30ms so the claim write is comfortably inside the deadline on a slow DB; the shape is identical.) **How acceptance was checked** - The new test passes, and it genuinely discriminates: with `settleCtx` reverted to `ctx` it fails with `effect status "running" after a cancelled reaction ... SettledAt:<nil>`; with the fix it passes. - `go test ./... -count=1` — all packages ok (e2e 26.8s) against the local Postgres. - `go vet ./...` clean. `gofmt -l` clean on every file I touched. One note: `gofmt -l` over the whole repo also flags `bus.go`, `otel.go`, and `internal/e2e/aggtable_test.go`. Those are pre-existing (struct-tag alignment, untouched by this change), so I left them alone rather than mixing an unrelated reformat into this PR.
