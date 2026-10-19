---
title: "ADR 0008 — loom generate writes one file per aggregate and record, deterministically"
status: accepted
date: 2026-10-19
summary: loomgen holds one <snake name>_gen.go per aggregate and per record (its state, commands, events, fold, Handlers interface, def function, binding method on Impl and its registration) beside shared models_gen.go and registry_gen.go that name no aggregate or record, so adding one adds only its own file; NewRegistry adds the registered owners in name order; render output depends only on the schema's ordered slices, so identical schemas give identical bytes.
---

# ADR 0008 — loom generate writes one file per aggregate and record, deterministically

## Context

ADR 0003 made a schema a directory, so a feature that adds an aggregate
adds a schema file instead of inserting into a shared one. The generated
code undid that. `loom generate` wrote everything into two files,
`loomgen/models_gen.go` and `loomgen/registry_gen.go`. Every aggregate's
structs, handler interface and registry entry went into the same two files,
in name order. Two branches that each add an aggregate both insert into
those files, often at the same place, so their branches conflict in
generated code that neither author wrote.

`generate --check` (ADR 0005) compares rendered bytes with disk. That only
works if rendering is a pure function of the schema. A file set split by
aggregate needs the same guarantee for which files exist and what each one
holds.

## Decision

- **One file per aggregate and per record.** `loomgen/<snake name>_gen.go`
  (`toSnake` of the declared name, so `LedgerEntry` → `ledger_entry_gen.go`)
  holds that owner's:
  - commands, with their `LoomCommand` and generated `Validate`;
  - events that only it emits (see below);
  - state struct and, for an aggregate, its `Fold`;
  - `<Name>Handlers` interface;
  - a function that returns its definition, `aggregateDef<Name>(h
    <Name>Handlers) *loom.AggregateDef` or `recordDef<Name>(h
    <Name>Handlers) *loom.RecordDef`. Its `Handle` closures call through
    `h`;
  - its binding method on `Impl` and its registration (below).
- **An event belongs to the one owner that emits it.** If the commands of
  exactly one aggregate or record emit an event, the event goes in that
  owner's file. An event emitted by several owners, or by none (a
  `@retired` event, a consumed foreign event, an event only reactors
  read), stays shared.
- **What stays shared, and why.** These declarations belong to no single
  aggregate, or they wire everything together:
  - `models_gen.go`: enums, named types, shared events, entities (and
    their folds), series.
  - `registry_gen.go`: the `loomErrNilEvent`, `asAny` and
    `checkDispatches` helpers, the reactors' `Reactions` and the `@fold`
    projections' `Folds` interfaces, `EventUpcasts`, the `Impl` struct,
    `NewRegistry`, and the registration table below.
  - `tables_gen.sql`: the DDL.

  None of these names an aggregate or a record. `Impl` has a field per
  reactor and `@fold` projection (and `Upcasts`), not per aggregate.
  `NewRegistry` lists the shared events (emitted by several owners, by
  none, or foreign), the entities' tables, the reactors and the
  projections; everything an aggregate or record contributes comes from
  its own file.
- **Each owner registers itself; `NewRegistry` reads the table.**
  - `registry_gen.go` declares an unexported table, `loomOwners`, of
    `loomOwner{kind, name, register}`, and `loomRegister` to append to it.
  - Each `<name>_gen.go` has an `init` that calls `loomRegister` with
    its kind (`"aggregate"` or `"record"`), its name, and a `register`
    function. That function appends the owner's def
    (`aggregateDef<Name>(h.(<Name>Handlers))`) to `Aggregates` or
    `Records`, and appends the `EventDef`s of the events only it emits,
    its `upload` entries and, for an `@table` aggregate, its `TableDef`.
  - The same file declares the binding method
    `func (impl *Impl) <Name>(h <Name>Handlers)`. It stores `h` in an
    unexported map on `Impl`, keyed by name. The method has the
    aggregate's own name, so ADR 0006's rule still holds: a reactor named
    like an aggregate takes its kind as its `Impl` field
    (`FleetStallProcess`) and both compile.
  - `NewRegistry(impl Impl)` builds the shared parts, then
    `loomRegisterOwners` sorts a copy of the table by kind (aggregates
    before records) and then by name, and calls each owner's `register`.
    It never relies on `init` order. It then sorts `Events` by name, so
    the registry has the same order it had when one function listed
    everything.
  - An owner whose handlers were never bound panics at `NewRegistry`
    with its kind and name and the call to add:
    `loomgen: aggregate Payee has no handlers — call impl.Payee(...)`.
    That is the boot failure a `registry.go` written before the owner
    existed meets. It is loud and immediate, where a nil `Impl` field
    used to fail on the first command.
  - The hand-owned `registry.go` stub fills the reactor fields in an
    `Impl` literal, then calls one binding method per aggregate and
    record, then returns `loomgen.NewRegistry(impl)`:

    ```go
    impl := loomgen.Impl{ShipOnPayment: &ShipOnPayment{}}
    impl.Order(&Order{})
    return loomgen.NewRegistry(impl)
    ```

  Binding is typed: passing a value that lacks a handler method is a
  compile error naming the method, as before. We chose this over
  `Impl` holding `[]any` that `NewRegistry` matches by type assertion,
  which loses that error. We chose it over registering handlers in a
  package-level variable, which a test building two registries would
  share.
- **File names never collide.** An owner whose snake name is already taken
  gets its kind appended: `models` and `registry` are taken by the shared
  files, and two declared names can snake alike (`ABc`, `Abc`). So
  `aggregate Registry` is written to `registry_aggregate_gen.go`. If that
  name is taken too, a number is appended. Names are assigned in schema
  order, aggregates first, then records.
- **Determinism.** Rendering must return identical bytes for identical
  schemas:
  - Every file's content, and the set of files, comes only from the
    schema's ordered slices. Those are the declarations after `Sort`,
    which is by name.
  - A map may be used for lookups and set membership. Its iteration order
    must never reach the output. Where a set's members are written out,
    they go through `sortedKeys`.
  - `Render` returns a map of path → bytes, so callers that walk it sort
    the paths when order matters (`Check` and `Generate` sort their
    lists).
  - `TestGenerateIsDeterministic` renders twice, and generates into two
    directories, and compares the bytes.

`Render`, `Check` and `Generate` keep their signatures.

## Consequences

- A feature that adds an aggregate or a record adds one new generated
  file and changes no existing one. Two branches that each add one touch
  disjoint files in `loomgen/` and cannot conflict there
  (`TestTwoAggregatesAddedTouchDisjointFiles`). A command added to an
  existing aggregate changes only that aggregate's file. The hand-owned
  `registry.go` still gets one binding line per aggregate. It is the
  developer's file, and the line is theirs to add.
- Some additions still touch a shared file, because what they add is
  shared:
  - an `@table` aggregate adds its DDL to `tables_gen.sql`;
  - an event that gains a second emitter moves to `models_gen.go`;
  - the first event with an `upcast` adds `EventUpcasts` and
    `Impl.Upcasts`;
  - an aggregate named like an existing reactor renames that reactor's
    `Impl` field (ADR 0006).
- Regenerating an existing service moves code out of `models_gen.go` and
  `registry_gen.go` into new files. It renames no type, and Handlers
  interfaces keep their names. The aggregate and record fields are gone
  from `Impl`, though, so a `registry.go` that sets them
  (`Impl{Order: &Order{}}`) no longer compiles. Move each such field to a
  binding call, `impl.Order(&Order{})`, as the stub now writes it. The
  reactor fields stay in the literal.
- An event that gains a second emitter moves from its owner's file to
  `models_gen.go`. This is rare. It is the cost of not having an event
  owner in the schema.
- `generate --check` reports a per-aggregate file as missing or stale just
  like a shared one. `generate` removes a generated file that render no
  longer produces (a renamed or deleted aggregate's, recognised by loom's
  header) and prints `removed <path>`. `--check` names such a file stale.
  A file without the header, or a stub, is never touched.
