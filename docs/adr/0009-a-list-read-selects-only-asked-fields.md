---
title: "ADR 0009 — a list read selects only the fields it asks"
status: accepted
date: 2026-10-25
summary: loom.Query gains an opt-in Fields []string; nil/empty keeps the whole-row read byte-identical, naming fields narrows the SQL to those columns (@table) or that jsonb path, and the GraphQL list resolvers opt in automatically from the list field's own selection, so a query that asks for `status` never pays to read a TOASTed `heavy` column it did not ask for.
---

# ADR 0009 — a list read selects only the fields it asks

## Context

Every list read — `QueryEntities`, `QueryRecords`, and GraphQL's
`{x}s`/`{x}sChanged` fields over them — read the whole state document per
row (`to_jsonb(t)` for a `@table` entity, `data` for the jsonb-document
path) regardless of which fields the caller actually used. A state with a
wide column — a long description, an embedded document, anything large
enough that Postgres stores it out of line (TOASTed) — is read and
(for `@table`) rendered into every page, even when a list view only shows
a status and a total.

GraphQL makes the waste easy to hit by accident: a list field's selection
set already says exactly which fields the caller wants, but nothing told
the SQL layer. A dashboard listing `{ orders { id status } }` paid to read
and decode every order's full state on every page.

The measurement in `docs/measurements/list-read-buffers.md` quantifies the
TOASTed case for a `@table` read: a page of 50 rows with an 8192-byte
`heavy` column reads 60360 shared buffers for the whole row and 360 for
`status` alone — a 0.0060 ratio, well under the "under a tenth" bar this
goal set.

## Decision

- **`loom.Query` gains `Fields []string`, opt-in** (`query.go`). Nil or
  empty `Fields` is the existing whole-row read: both list-SQL builders
  (`tableListSQL` for `@table`, `docsListSQL` for the jsonb-document path)
  emit byte-identical SQL to before this change, so no existing caller or
  test sees a different query.
- **Naming fields narrows the SELECT, not the row shape.** When `Fields`
  is set:
  - `tableListSQL` emits `jsonb_build_object(...)` over only the named
    real columns instead of `to_jsonb(t)`, so an unasked TOASTed column is
    never read off disk or detoasted by Postgres.
  - `docsListSQL` emits `jsonb_build_object('f', data->'f'), ...)` over
    only the named keys of the jsonb `data` column. This narrows the
    *output* and the wire/decode cost the same way, but Postgres still
    reads and detoasts the whole `data` value to project out of it — a
    TOASTed jsonb document has no server-side saving on this path, only a
    client-side one. A caller that needs the server-side saving on a large
    field needs a `@table` entity.
  - Both builders validate every named field against the real schema
    (`TableDef.Columns` for `@table`, `fieldRe` plus the jsonb shape for
    the doc path) before building SQL: an unknown field is
    `loom: bad field %q`, a parameterization error, not a SQL injection
    surface.
  - The meta fields (`id`, `namespace`, `updated_at`) always ride the
    `Row` outside `Data`; naming one in `Fields` is accepted and ignored
    (`metaField`). `Filters` and `OrderBy` may still name fields `Fields`
    leaves out of the document — narrowing what comes back in `Data`
    never narrows what can be filtered or sorted on.
  - More than 50 named fields chunk into multiple `jsonb_build_object`
    calls joined with `||`, because a Postgres function call caps at 100
    arguments (50 key/value pairs).
- **The GraphQL list resolvers opt in from the list field's own
  selection, automatically.** `listFields` (`graphql/graphql.go`) walks a
  list field's `ResolveParams`, following inline fragments and named
  fragment spreads, and returns the top-level state fields the query
  actually asked for — by state name (`camelToSnake` of the GraphQL
  name), excluding the meta fields and `__typename`, keeping only a
  nested object's own top-level name. `docList` and `listChanged` (the
  `{x}sChanged` subscription's initial read and every requery) set
  `q.Fields = listFields(p)` right after building the rest of the query,
  so a selection naming only meta fields keeps `Fields` nil — the
  whole-row default — and any other selection narrows it. No schema or
  resolver signature changed; a client sees no difference beyond the
  query running cheaper.
- **`camelToSnake` is a heuristic, not a verified inverse of every
  `json` tag.** It handles the common case (`dueDate` → `due_date`,
  `totalCents` → `total_cents`) but does not round-trip a tag with digits
  after an underscore or an uppercase/acronym tag. A mismatch surfaces
  loudly as the SQL layer's own `loom: bad field` validation error — it
  cannot silently select the wrong column — but it can wrongly turn away
  a field that is in fact valid. Mapping the selection through the
  generated type's own field tags, rather than re-deriving snake_case,
  is the fix if a schema trips this; it was not needed by any aggregate
  this change shipped against.

## Consequences

- A list read is, by default, exactly as expensive as it was before this
  change: `Fields` is opt-in and nil/empty preserves the old SQL
  byte-for-byte (unit-tested).
- A `@table` entity's list reads get materially cheaper when a caller
  (GraphQL or a direct `Query`) asks for a subset of fields, in
  proportion to how much of the row the unasked columns hold — the
  TOASTed-column case this ADR's measurement captures, where the ratio
  was 0.0060 for a page of 50 rows.
- The jsonb-document list path (no `@table`) gets a smaller, client-side
  saving only: less to transmit and decode, not fewer buffers read,
  because projecting out of `data` still reads the whole value server
  side. An entity whose read-heavy state is actually large should move
  to `@table` to get the server-side saving too.
- Every list read still returns `id`, `namespace`, and `updated_at` on
  every `Row` regardless of `Fields`, so pagination, ordering, and
  `{x}Changed` diffing keep working unchanged.
- `Filters` and `OrderBy` are independent of `Fields`: a caller can sort
  or filter on a field it never asks to see in `Data`.
