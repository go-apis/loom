---
title: "ADR 0019 — a runner writes its checkpoint only when it matters"
status: accepted
date: 2026-11-23
summary: A projection or process runner keeps the position it reaches by passing events it does not subscribe to in memory; it writes its row with every event it applies, and the client's flusher writes every runner's passed position in one statement every Config.CheckpointEvery (1s), reading every row back; every write is conditional on the row the runner last knew; an idle step is free, or one statement with no transaction when the runner does not know where it is; AwaitProjection and the new RunnersBehind count only the events a runner subscribes to.
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
   runner's loop owns its position and steps from it. The runner writes
   its own row with every event it applies, as before: a projection's
   fold commits with the write, and a process writes after its reaction.
   It also writes once when it has no row yet.
2. **One flusher writes what runners passed.** Every
   `Config.CheckpointEvery` (default one second), the client writes the
   position of every runner that has passed events its row does not
   hold yet. All of these go in one statement (`flushCheckpoints`),
   which also reads every runner's row back.
   - A runner's row therefore moves at most once a `CheckpointEvery`
     while it passes events. It reaches where the runner stopped within
     one interval of the runner going idle.
   - A runner holds its position's lock for the whole of a step. The
     flusher only tries the lock, so it never writes beside a step; a
     runner mid-step waits for the next flush.
   - With about 200 runners on a service, a write per runner per second
     was still 200 statements a second while events trickled in. The
     flush makes that one.
3. **Every write is guarded.** `passCheckpoint` and the flush move a row
   only from the value the runner last wrote or read. If anything else
   moved the row, the write misses and the runner reads the row again: a
   rebuild, an operator's skip, a reset, a rewind, or another instance's
   step. A projection's folds commit only with their write, so folds
   from a stale position roll back.
   - On the same instance, those movers also set the runner's `resync`
     flag (`wakeAll`, `kickRunner`).
   - The flush's read catches movers on other instances within an
     interval.
4. **An idle step costs one statement, or none.** A runner that knows
   where it is, with nothing past it in the fan-out, sends nothing. A
   runner that does not know where it is (it has just started, been
   resynced, or had a write miss) sends `probeCheckpoint`: one
   statement, with no transaction around it. The statement returns:
   - the projection's advisory try-lock, taken and released within the
     statement;
   - the checkpoint row;
   - whether any event lies past the position.

   A batch the projection folds nothing of is passed without a
   transaction.
5. **Readers that wait count only what a runner applies.**
   - `AwaitProjection` treats a checkpoint short of the awaited sequence
     as reached when none of the events in between are of a type the
     projection folds, and the sequence is in the log.
   - `RunnersBehind(seq)` names the runners whose row is short of `seq`
     by an event they subscribe to. A row of a runner the registry does
     not declare is named whenever it is short.
   - A test harness that used to wait for "every row at the head" asks
     `RunnersBehind` instead. Every row at the head is now true only
     within an interval of the log going quiet.

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
most `CheckpointEvery` behind the position it has reached, plus the
step it is in. The lag covers only events the
runner ignores.

A row that has not reached the head a few seconds after the log stopped
moving is a stopped runner. loom's own lag hook (`hooks.go`) measures
from the next subscribed event past the row, so the bound does not move
it at all.

## Consequences

- On runsheet's e2e shard 1/6, the PR gives the before and after.
- A test (`TestTwentyRunnersWriteTheirCheckpointsOnlyWhenItMatters`)
  runs 20 runners over 1,000 events arriving a few at a time, each
  runner subscribed to 5% of them. It holds the writes to at most two
  an event, and covers a crash and a restart.
- `TestAnIdleRunnerStepIsOneStatement` shows the one-statement look,
  the free idle step and the one-statement flush.
- A reader that waits for a checkpoint to reach the head now waits up
  to `CheckpointEvery` longer when the head is an event the runner
  ignores. While events keep arriving faster than that, such a reader
  may wait forever. Wait with `AwaitProjection` or `RunnersBehind`.
- A crash re-passes up to `CheckpointEvery` of ignored events. That
  costs reads, not reactions.
