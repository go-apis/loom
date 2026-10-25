---
title: "ADR 0009 — a projection declares its indexes"
status: accepted
date: 2026-10-25
summary: A @table entity declares secondary indexes with SDL `index (col [desc] [nulls first|last], ...)` lines; the generator emits them as TableDef.Indexes and in tables_gen.sql; Migrate builds each CONCURRENTLY on its own connection outside any transaction, drops and retries an invalid one (pg_index.indisvalid), and is a no-op the second time; Rebuild and Reset empty with DELETE so indexes survive.
---

# ADR 0009 — a projection declares its indexes

## Context

A `@table` entity gets a typed table with one primary key,
`(service, namespace, id)`. Its list reads filter and sort on other
columns, and nothing in loom could declare an index for them, so a
consumer either lived with a sequential scan or added an index by hand
outside the schema, where `Migrate` did not know about it, `generate
--check` could not see it, and a fresh database did not get it.

The cost was measured on runsheet production on 2026-10-24. Page reads of
`loom_t_platform_work_item_index` cost 79,390 buffers per call before an
index on `(service, namespace, org_id, updated_at DESC NULLS LAST)`, and
1,282 after: about 62 times fewer.

## Decision

**The index is part of the projection's schema.**

- **SDL.** Inside `entity X @table { ... }`, a line
  `index (col [desc] [nulls first|last], ...)` declares a secondary index.
  Columns are the entity's state fields or the meta columns `service`,
  `namespace`, `id` and `updated_at`, in the order written. The SDL gives
  no name. It defaults deterministically to `idx_` + snake_case(entity) +
  `_` + the column names joined by `_`. Validation refuses an index on an
  entity without `@table`, a column the entity does not have (naming both),
  and two indexes with identical column lists.
- **Generated code.** `loom.TableDef` gains `Indexes []TableIndex{Name,
  DDL}`. Each DDL is
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS <name> ON <table> ("col" [DESC] [NULLS FIRST|LAST], ...);`.
  The same statement is written to `loomgen/tables_gen.sql`, right after
  the table's CREATE TABLE, in declared order. `loom generate --check`
  needed no change. Its byte compare ([ADR 0005](0005-generator-is-pinned-and-checked.md))
  already reports `registry_gen.go` and `tables_gen.sql` stale when only
  an index differs.
- **Migrate builds each index concurrently, on its own connection.**
  `CONCURRENTLY` keeps writes flowing while a large live table is indexed,
  but Postgres refuses it inside a transaction block. So `Migrate` creates
  indexes after the table DDL and the column diff (`migrateIndexes`). Each
  index runs autocommitted on a connection taken with `pool.Acquire`. No
  transaction is ever open around it, in the spirit of
  [ADR 0002](0002-no-transaction-across-a-dispatch.md): a long build holds
  no lock that anyone else is waiting on through loom.
- **An invalid index is replaced, with a bound.** A concurrent build that
  fails or is interrupted leaves an index with `pg_index.indisvalid =
  false`. `IF NOT EXISTS` would skip it forever, and the planner never uses
  it. Before each create, `Migrate` looks the name up in `pg_index` in the
  current schema:
  - valid: nothing to do, so running `Migrate` twice is a no-op;
  - invalid: `DROP INDEX CONCURRENTLY`, then create again;
  - after 3 attempts, `Migrate` fails with an error naming the index, the
    table and the last cause.
- **Rebuild and Reset keep indexes.** `Rebuild` empties a read model with
  `DELETE … WHERE service`, and `Reset` empties every loom table with
  `DELETE`. Neither truncates or drops. That keeps the indexes, which can
  take a long time to rebuild on a large table, and keeps the rows of
  other services that share a table.

## Consequences

- A consumer adds an index by editing the schema and regenerating, and the
  deploy-time `Migrate` builds it without blocking writes. A fresh database
  gets the same indexes as production.
- Indexes are matched by name only. A changed column list gets a new
  default name, so `Migrate` builds the new index and leaves the old one in
  place. Dropping it is the operator's call, the same as an extra column
  under the additive-only diff.
- `Migrate` holds one extra pool connection while it builds indexes. A
  build on a large table takes as long as Postgres needs, and `Migrate`
  runs at deploy time, not at boot.
- `Reset` now deletes rows instead of truncating. That is slower on a very
  large store and leaves dead tuples for autovacuum. In exchange, Reset
  takes no `ACCESS EXCLUSIVE` lock. Sequences were not restarted before
  and are not now.
- Indexes on aggregate `@table` mirrors and on series are out of scope.
  Validation refuses the first; series keep their generated keys.
- The number 0009 is shared with
  [ADR 0009 — a list read selects only the fields it asks](0009-a-list-read-selects-only-asked-fields.md),
  which merged first. Both are accepted, and each is cited by its full
  file name.
