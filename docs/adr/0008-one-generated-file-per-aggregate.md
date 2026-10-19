---
title: "ADR 0008 — loom generate writes one file per aggregate and record, deterministically"
status: accepted
date: 2026-10-19
summary: loomgen holds one <snake name>_gen.go per aggregate and per record (its state, commands, events, fold, Handlers interface and def function) beside the shared models_gen.go and registry_gen.go; render output depends only on the schema's ordered slices, never on map iteration, so identical schemas give identical bytes.
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
    `h`.
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
    and `NewRegistry`.
  - `tables_gen.sql`: the DDL.

  `NewRegistry` still lists every event, projection, table, upload and
  reactor. It now builds each aggregate and record from its def function,
  `aggregateDefOrder(impl.Order)`, and no longer writes the definition
  inline. Each aggregate still has one line in `Impl` and one in
  `NewRegistry`. Removing those lines is follow-up work.
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

- A feature that adds an aggregate adds one new generated file. It changes
  the shared files only by its `Impl` field, its `NewRegistry` line and the
  `EventDefs` of its events. A command added to an existing aggregate
  changes only that aggregate's file.
- Regenerating an existing service moves code out of `models_gen.go` and
  `registry_gen.go` into new files. It renames no Go identifier, so stubs
  and hand-written code compile unchanged. Handlers interfaces keep their
  names.
- An event that gains a second emitter moves from its owner's file to
  `models_gen.go`. This is rare. It is the cost of not having an event
  owner in the schema.
- `generate --check` reports a per-aggregate file as missing or stale just
  like a shared one. A `_gen.go` left behind by a removed aggregate is not
  yet detected or removed; that is follow-up work.
