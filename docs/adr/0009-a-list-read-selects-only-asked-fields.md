---
title: "ADR 0009 — a list read selects only asked fields, opt-in via Query.Fields"
status: accepted
date: 2026-10-24
summary: Query gains an opt-in Fields []string; nil/empty keeps the byte-identical whole-row read, and a @table list read with Fields set emits jsonb_build_object over only the named columns, so an unasked TOASTed column is never detoasted — GraphQL's list resolvers derive Fields from the selection set and opt in automatically.
---

# ADR 0009 — a list read selects only asked fields, opt-in via Query.Fields

## Context

Every list read — `queryTable` over a `@table` entity, `queryDocs` over
the jsonb-entity store — built one SQL statement that read the whole
state document per row: `to_jsonb(t) - 'service' - 'namespace' - 'id' -
'updated_at'` for a typed table, `data` for the jsonb path. A caller
asking for a 50-row page to show two columns in a list still paid to read
and decode every column of every row in that page, including any large
or TOASTed one.

A `@table` entity can declare a large `text` column (an embedded document,
a rendered report, a blob of free-form notes). Postgres stores a value
over roughly 2KB out of line (TOASTed); `to_jsonb(t)` forces every
TOASTed column of every selected row to be fetched and decompressed, even
when the caller only reads `status` off each row. A GraphQL list field
whose selection names two scalar fields triggers the same full detoast
for every row on the page.

`Query` (`query.go`) already carries `Filters`, `OrderBy`, `Limit` and
`Offset` — the read is already not "give me everything," it is already
shaped by what the caller asked for. Fields were the one part of the row
shape Query could not narrow.

## Decision

- **`Query.Fields []string` is opt-in.** Nil or empty gives the exact
  whole-row SQL the read built before this change, byte for byte — every
  existing caller is unaffected. Setting `Fields` narrows `Row.Data` to
  just those state fields.
- **The meta fields always ride the `Row` outside `Data`.** `id`,
  `namespace` and `updated_at` are returned by every list read regardless
  of `Fields`; naming one in `Fields` is accepted but a no-op
  (`metaField`, `query.go`).
- **An unknown field is a build-time error**, `loom: bad field %q`, not a
  silently empty result: a `@table` read checks the name against
  `TableDef.Columns`, the jsonb-entity read against the same `fieldRe`
  pattern used elsewhere. `Filters` and `OrderBy` may still name a field
  `Fields` leaves out — narrowing the returned document does not narrow
  what the read can filter or sort by.
- **The `@table` read emits `jsonb_build_object` over only the named
  columns** instead of `to_jsonb(t)`, so an unasked column's TOAST
  pointer is never resolved — the saving is on disk/buffer I/O, not just
  on the wire. Past 50 named fields the object is built in chunks of 50
  and concatenated with `||`, since a Postgres function call caps at 100
  arguments.
- **The jsonb-entity read narrows what it selects from `data`**
  (`jsonb_build_object('f', data->'f', ...)`), but this is a wire and
  decoding saving only: `data` is one jsonb column, and Postgres must
  detoast the whole value before it can apply the `->` operator to any
  key inside it. A service that wants the TOAST-avoidance in addition to
  the wire saving needs a `@table` entity, not the jsonb path.
- **GraphQL opts in automatically.** A list field's resolver derives
  `Fields` from its own selection set (`listFields`, `graphql/graphql.go`)
  — the top-level scalar/enum names it asks for, meta fields excluded,
  fragments followed — and passes it as `Query.Fields`. A selection that
  asks only for meta fields (`id`, `namespace`, `updatedAt`) yields a nil
  `Fields`, which keeps the whole-row default; so does a selection naming
  any field the list's rows do not store — a join (whose resolver also
  needs its `Via` field off the row) or a `@pii` field with no column.
  No GraphQL schema or
  gateway contract changed: this only narrows what loom reads to satisfy
  a selection that was already fully specified by the query.

## Consequences

- A list read that only needs a few fields no longer pays to detoast or
  decode the rest; `docs/measurements/list-read-buffers.md` records an
  EXPLAIN (ANALYZE, BUFFERS) comparison on a `@table` entity with an 8KB
  TOASTed column, where the asked-fields page read a small fraction of
  the whole-row page's shared buffers.
- Every existing caller of `Query` is unaffected: `Fields` defaults to
  nil, and the whole-row SQL is unchanged (covered by
  `TestListSQLOmitsUnaskedColumns`, which asserts the nil/empty-`Fields`
  SQL string matches the pre-change string exactly).
- A caller that wants the TOAST-avoidance, not just the wire saving, must
  use a `@table` entity for that state — the jsonb-entity path's `data`
  column still detoasts in full regardless of `Fields`.
- A field name that does not match the entity's declared columns (or, on
  the jsonb path, the field-name pattern) is rejected before any SQL
  runs, the same way an unknown filter or order field already was.
- GraphQL's selection-to-`Fields` mapping goes through `camelToSnake`,
  which is not an exact inverse of every possible `json` tag (a tag with
  a digit after an underscore, or an uppercase/acronym tag, does not
  round-trip). A schema whose state fields all follow the generator's
  plain `snake_case` naming is unaffected; a hand-written `json` tag
  outside that convention should be checked against `loom: bad field`
  rejections if its list field stops returning that value.
