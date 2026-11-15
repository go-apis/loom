---
title: "ADR 0012 — a loop is declared per aggregate, and its wait is built in"
status: accepted
date: 2026-11-15
summary: "@loop on an aggregate declares one recurring look for the aggregate TYPE, not its instances; loom generates a service-wide Look interface with one method per looping aggregate (mirroring EventUpcasts/Upcasts) and a once-only stub; the wait between calls is the runtime's to keep, built in, so an implementation never hand-rolls the backoff the way a process does with NotYet and @retry."
---

# ADR 0012 — a loop is declared per aggregate, and its wait is built in

## Context

Loom already has three ways to make something recur: a `schedule ... every
... -> Command` dispatches a command on an interval to one stream;
`cli.Every` calls a function on an interval, once on the newest generation
alive; `loom.NotYet` with a process's `@retry` lets a reaction that has
nothing to do yet look again later, backing off between the reactor's
declared bounds. None of them fit an aggregate that needs its own recurring
look across all of its instances — a watchdog over every `Order`, say, not
one event's reaction and not a fixed interval's tick to a single stream.
Hand-rolling it today means a process subscribed to nothing useful, or a
service's own goroutine with its own ticker and its own backoff — exactly
the kind of hand-rolled wait `NotYet`/`@retry` exists to spare a reaction.

## Decision

**`@loop` on an aggregate declares that aggregate's type one recurring
look.** It is per aggregate TYPE, not per instance: one method, not one per
row.

```
aggregate Order @loop {
  state { ... }
  ...
}
```

Records take no `@loop`: a record is bound state with nothing beyond
itself to look across; `@loop` belongs to aggregates. Declared on a
record, it is rejected at parse time as any other unknown directive is;
unlike most of SDL's unknown-directive rejections, this one names the file
and line, because the record path captures a position token before
consuming directives.

**Generation mirrors `EventUpcasts`/`Upcasts`** (the shared, no-owner
declaration that ADR 0008 carved out for a service-wide concern with no
kind package of its own). Every aggregate declaring `@loop` gets a method,
`Look<Name>(ctx context.Context) ([]loom.Command, error)`, on one
service-wide interface, `Look`, written to `registry_gen.go` only when at
least one aggregate loops. `Impl` gets a `Look Look` field, and
`NewRegistry` panics naming the stub when the schema declares `@loop` and
`Impl.Look` is nil — the same loud, immediate boot failure an unbound
aggregate or record gets. A once-only stub, `look.go`, holds `type Look
struct{}` and one panic-stub method per looping aggregate, wired into
`registry.go` (`impl.Look = &Look{}`, or `Look: &Look{}` in the `Impl{}`
literal under `wire:`) the same way `Upcasts` is.

**The wait between calls is the runtime's to keep, not the
implementation's to hand-roll.** Each `Look<Name>` method returns the
commands to dispatch, or none when there is nothing to do yet — closer to
a schedule's tick than to a reaction answering `NotYet`. The interval and
its backoff are declared nowhere in `Look<Name>`'s own body, so an
implementation never writes its own ticker or its own `NotYet`/`@retry`
pair to get a recurring look. This change is the parse- and generate-time
half of that: the SDL directive, the schema field, and the generated
interface and stub. The runtime that actually calls `Look<Name>` on an
interval and owns the wait between calls is not part of it (tracked in
DESIGN.md's "Not yet built").

## Consequences

- `schema.Aggregate.Loop` carries the directive; declaring `@loop` on a
  record is refused at parse time, naming the file and line
  (`sdl/loop_test.go`).
- A schema with no `@loop` aggregate generates no `Look` interface, no
  `Impl.Look` field, and no stub (`gen/loop_test.go`,
  `TestLoopAbsentOmitsLookInterface`).
- A schema with a `@loop` aggregate generates the interface, the `Impl`
  field, the nil-check panic, and the once-only `look.go` stub; a second
  `generate --check` on the regenerated fixture is clean
  (`gen/loop_test.go`, `TestLoopEmitsLookInterfaceAndStub`).
- `Look`, like `Upcasts`, is not wireable by `wire:` convention — no kind
  package holds it, so it stays the caller's to set in `registry.go`
  (`gen/wire.go`).
- Still to build, tracked on the issue: the runtime that calls each
  `Look<Name>` on an interval and waits between calls.


## Since

- **When to look is a pure rule** (`loop.go`). `LoopState` is what the
  runtime has noted about a loop since its last look — when that was,
  when the newest event arrived, what it is waiting on — and
  `LoopState.NextLook` answers the earliest of `LoopEvery` (a minute)
  after the last look, `LoopSoon` (a second) after an event that has
  arrived since, and the first deadline among its waits. It reads no
  clock and no store, so the runner and a test apply the same rule.
- **A `Wait` has a name, an owner and a deadline.** A look is told why it
  was called through `loom.LookingAt(ctx)`: whether an event arrived, the
  waits that have expired and those still running. An expired wait is
  told once and then dropped.
- **`loomtest.Loop`** runs a look that way with no database: it says when
  the look would next be made, calls it as the runtime would, runs the
  commands it answers with through the registry's handlers and folds
  their events into the reader's state, so a second look sees what the
  first did (`loomtest/loop_test.go`).
- Still to build: the runner that keeps `LoopState` in Postgres and
  calls each look, and how a look sets a wait.
