---
title: "ADR 0016 — an aggregate folds every event it declares"
status: accepted
date: 2026-11-17
summary: An aggregate's generated Fold has a case for every event its commands emit and for every event declared in its block that is not @retired, so deleting the last command that emits an event keeps the event's fold and its stored rows replay as before; @retired stays the only way to take a fold away, and no new annotation is needed.
---

# ADR 0016 — an aggregate folds every event it declares

## Context

`loom generate` built an aggregate's `Fold` from its commands' emit
lists. An event had a case in the fold only while some command of the
aggregate still emitted it. To stop producing an event, a service
deletes or changes the command that emits it, and the declaration stays,
because the log still holds the event's rows. That deletion also removed
the event's case from the fold, and nothing said so. A replay then
decoded the rows and folded nothing from them, so the state read as it
had before each such event.

This happened in runsheet. When its automatic kind breaker was removed
(runsheet #6857), `KindBreaker` lost the folds of `KindProbeStarted`,
`KindProbeFailed`, `KindProbeDue` and `KindProbeSkipped`, and
`KindFailureWindow` lost the fold of `KindFailureRecorded`, its only
event. The schema comments say those events stay "so stored events
load". They loaded, but they no longer folded. Next, runsheet stops
emitting about fifteen `WorkItem` events that carry `status`. Their rows
must keep setting the status on replay. The workaround was a command
whose handler always refuses, kept only for its emit list.

## Decision

- **An aggregate folds every event it declares.** Its state folds every
  event its commands emit and every event declared inside its block that
  is not `@retired`, whether or not a command emits it. The parser
  records the events declared in each aggregate's block
  (`schema.Aggregate.Events`). The generator folds the union of those
  and the emit lists (`aggregateEvents`).
- **Stopping an event is deleting its emitter.** Keep the declaration in
  the aggregate's block. The event keeps its fold, so its rows replay as
  they did while a command emitted it. Projections and processes may
  still subscribe to it, as before.
- **`@retired` is the only way to take a fold away.** A retired event's
  rows decode and fold nothing. Nothing may emit it, subscribe to it or
  upcast it (unchanged). So no new annotation is needed:
  - an event declared in the block and not retired is kept for reading,
    and folded;
  - an event marked `@retired` is kept so its rows decode, and not
    folded.
- **Only the block counts.** An event declared outside every aggregate
  (at the top level, or in a record) is folded by an aggregate only if
  one of that aggregate's commands emits it, as before.
- **Where the struct lives does not change.** ADR 0008 still applies. An
  event that no command emits stays in its schema file's shared
  `_types_gen.go`, and its fold case is in the aggregate's file.

## Consequences

- Regenerating can add fold cases to an aggregate that declares events
  nothing emits. Each case restores what replay did while the event was
  emitted. If an aggregate's state should no longer take such an event's
  fields, mark the event `@retired`. That is only possible when nothing
  subscribes to it. For runsheet, regenerating adds the four `KindProbe*`
  cases to `KindBreaker` and `KindFailureRecorded` to
  `KindFailureWindow`.
- A snapshot taken while a fold was missing holds the state without that
  event's fields. A load that starts from that snapshot does not fold the
  earlier rows again. Replaying the stream from version 0 does.
- If an event is declared in one aggregate's block but emitted by
  another aggregate's command, both aggregates fold it. Its rows are only
  on the emitter's streams, so the declaring aggregate's case never runs.
- `TestAnEventNothingEmitsStillFolds` (in `gen` and in `internal/e2e`)
  holds this. The e2e test replays a stored `OrderDispatched`, which no
  command emits, to a shipped order.
