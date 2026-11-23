---
title: "ADR 0019 — a runner writes its checkpoint only when it matters"
status: accepted
date: 2026-11-23
summary: A projection or process runner keeps the position it reaches by passing events it does not subscribe to in memory, and writes it with the next event it applies, at most every Config.CheckpointEvery (1s) while passing, and within that bound of going idle; every write is conditional on the row the runner last knew; an idle step is free, or one statement with no transaction when the row is due a look; AwaitProjection counts only the events a projection folds.
---

# ADR 0019 — a runner writes its checkpoint only when it matters

## Context

Every projection and process reads its whole service's log and applies
a few types of it. Since the 2026-11-18 amendment to ADR 0010 a run of
events a runner ignores costs one write, but only within one step. When
events arrive a few at a time, every step holds a run of one, so every
runner still wrote its checkpoint for nearly every event.

On 2026-11-21, one runsheet e2e shard sent Postgres 4.07M statements in
283s for about 19.5k events (pg_stat_statements):

| statements | what |
|---|---|
| 1.18M | the checkpoint read at the start of every step |
| 524k / 424k / 100k | begin / commit / rollback, one transaction a step |
| 493k | the projection step's advisory try-lock |
| 482k | `passCheckpoint`, passing events a process ignores |
| 419k | `saveCheckpoint`, after a projection's step |

That is about 46 checkpoint writes an event and 89% of the statements.
Each took about 0.03ms. The cost was the per-statement and transaction
overhead: 482 Postgres CPU-seconds against 143s executing. Production
loom services run the same loop against the shared database.

## Decision

1. **The position lives in memory** (`runnerPos`, `checkpoint.go`). Each
   runner's loop owns its position and steps from it. It writes the row
   in three cases:
   - with every event it applies (a projection's fold commits with the
     write, and a process writes after its reaction), as before;
   - while passing events it does not subscribe to, at most once every
     `Config.CheckpointEvery` (default one second);
   - when it goes idle holding a position it has not written. The write
     happens once that interval has passed since the last write.
     `runLogLoop` sleeps no longer than until then.
2. **Every write is guarded.** `passCheckpoint` moves the row only from
   the value this runner last wrote or read. If anything else moved the
   row, the write misses and the runner reads the row again: a rebuild,
   an operator's skip, a reset, a rewind, or another instance's step. A
   projection's folds commit only with that write, so folds from a stale
   position roll back. On the same instance, those movers also set the
   runner's `resync` flag (`wakeAll`, `kickRunner`). An idle runner looks
   at its row at most every `CheckpointEvery` to catch movers on other
   instances.
3. **An idle step costs one statement, or none.** When the fan-out shows
   nothing past the position and no look is due, the step sends nothing.
   When a look is due, `probeCheckpoint` is one statement, with no
   transaction around it. It returns the projection's advisory try-lock,
   taken and released inside the statement, the checkpoint row, and
   whether any event lies past the position. A batch the projection
   folds nothing of is passed without a transaction.
4. **`AwaitProjection` counts only what the projection folds.** A
   checkpoint short of the awaited sequence counts as reached when none
   of the events in between are of a type the projection folds, and the
   sequence is in the log. Read-your-writes therefore never waits out
   the bound in decision 1.

## Parity

The row holds sequence S only when every subscribed event at or before
S has been applied: handled, parked, or handed to a durable retry. This
holds because the in-memory position runs ahead of the row only over
events the runner ignores. Applying an event writes the row at that
event.

A crash between writes restarts from the row. The restarted runner
passes the unwritten tail again, which it would pass anyway, and no
handler runs twice. The overlap rules of ADR 0010 are unchanged, except
that the write after a handled event is now guarded too (see that ADR's
2026-11-23 amendment).

## Liveness bound

Some readers of `loom_checkpoints` tell a moving runner from a stopped
one: runsheet's platform watchdog and `/ops/runners`, loom's lag gauge
(`otel.go`), and `status.go`. For them, a running runner's row is at
most `CheckpointEvery` behind the position it has reached, plus one
step and any wait for a step slot. The lag covers only events the
runner ignores.

A row that has not reached the head a few seconds after the log stopped
moving is a stopped runner. loom's own lag hook (`hooks.go`) measures
from the next subscribed event past the row, so the bound does not move
it at all.

## Consequences

- runsheet's e2e shard: see the PR for the before and after.
  A test (`TestTwentyRunnersWriteTheirCheckpointsOnlyWhenItMatters`)
  runs 20 runners over 1,000 trickling events, each runner subscribed
  to 5% of them. It holds the writes to at most two an event, and
  checks a crash and a restart.
- A reader that waits for a checkpoint to reach the head waits up to
  `CheckpointEvery` longer when the head is an event the runner ignores.
  Such readers include a test harness's "settled" check. Waits for a
  projection should use `AwaitProjection`. A harness can lower
  `CheckpointEvery`.
- A crash re-passes up to `CheckpointEvery` of ignored events. That
  costs reads, not reactions.
