---
title: "ADR 0011 — a declared query generates its index"
status: accepted
date: 2026-11-14
summary: Inside entity X @table, a line `query (filter_col, ..., order_col [desc] [nulls first|last])` desugars at parse time into a schema.Index — the named filter columns, then the order column with its declared direction, then a trailing id column in the same direction for stable pagination — so it rides ADR 0009's generator/Migrate pipeline unchanged; restricted to entity @table, like index itself.
---

# ADR 0011 — a declared query generates its index

## Context

[ADR 0009 — a projection declares its indexes](0009-a-projection-declares-its-indexes.md)
let a `@table` entity declare `index (col [desc] [nulls first|last], ...)`
lines, so the index a list read needs ships with the schema instead of
living outside it, unknown to `Migrate` or `generate --check`.

That still asks the author to work out the index by hand from the read
they actually want: a filtered, ordered, paginated list — "orders with
this status, newest first" — needs not just the filter and order columns
but a trailing tie-breaker (here, `id`) in the order column's own
direction, or two rows with the same `updated_at` can be split across
pages or repeated. Getting the tie-breaker's direction wrong, or leaving
it out, is a silent correctness bug that only shows up as a flaky list
page under concurrent writes — exactly the kind of mistake a generator
should make impossible instead of a reviewer catching by hand.

## Decision

**The query is declared as itself, not translated into an index by
hand.**

- **SDL.** Inside `entity X @table { ... }`, a line
  `query (col, ..., order_col [desc] [nulls first|last])` declares a list
  query. All columns but the last are equality filters, named plain, in
  declared order. The last column is the order column, carrying whatever
  `desc`/`nulls first|last` it declares — the same per-column grammar
  `index` already parses (`sdl/parse.go`'s `indexColumns`, now shared by
  both). Columns are the entity's state fields or the meta columns
  `service`, `namespace`, `id`, `updated_at`, checked in
  `schema.Validate`, not at parse time, exactly as `index` is.
- **Desugars into an Index at parse time, nothing else.** `query` builds
  the same `schema.Index` a hand-written `index (...)` line would, with
  one addition: a trailing column named `id`, in the order column's own
  `Desc` direction, appended automatically as the stable tie-breaker. A
  `query (status, updated_at desc nulls last)` line produces exactly the
  `Index` that `index (status, updated_at desc nulls last, id desc)`
  would have, indistinguishable once parsed — same `Columns`, same
  default name (`defaultIndexName`, ADR 0009's `idx_<entity>_<col>_...`).
  It is appended straight into `Entity.Indexes`; no new field, no new
  runtime type.
- **The generator, `tables_gen.sql`, and `Migrate` need no change.**
  Because `query` only ever produces a `schema.Index`, ADR 0009's whole
  pipeline — `loom.TableDef.Indexes`, the `tables_gen.sql` `CREATE INDEX
  CONCURRENTLY IF NOT EXISTS`, `Migrate`'s build-outside-a-transaction and
  invalid-index replacement — runs over a declared query's index exactly
  as it runs over a hand-written one, unmodified.
- **Restricted to entity `@table`, like `index`.** `query` is only wired
  into the entity block's parser (`entityBlock`), not the aggregate
  block's, so it is unreachable on an aggregate `@table` the same way
  `index` is. `schema.Validate`'s existing index checks (only meaningful
  on `@table`, every column must be real, no two indexes naming the same
  columns the same way) cover a query-declared index for free, since by
  the time `Validate` runs it is just another `*schema.Index`.

## Consequences

- A consumer declares the read it wants — filter columns, then an order
  column and its direction — and gets a correct, deterministically-named,
  pagination-safe index without working out the tie-breaker by hand.
- A `query` line and a hand-written `index` line naming the same columns
  (filters, order column, and `id` in the matching direction) collide
  under `schema.Validate`'s "declared twice" check, same as two
  hand-written `index` lines would — the declared columns are the only
  thing either kind of line is remembered by.
- `query` is sugar, not a new concept the runtime has to know about:
  nothing downstream of parsing — `loom.TableDef`, `tables_gen.sql`,
  `Migrate`, `generate --check`'s byte compare — can tell a query-declared
  index from a hand-written one, so none of it changed.
- A query with only filter columns and no real order column (e.g.
  `query (status)`) still appends an ascending `id`, which is a legal
  index but not yet validated against any particular read pattern;
  nothing in this change recommends or rejects that shape.
