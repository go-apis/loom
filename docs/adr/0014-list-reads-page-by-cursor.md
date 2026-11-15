---
title: "ADR 0014 — list reads page by cursor with an id tie-breaker"
status: accepted
date: 2026-11-14
summary: tableListSQL and docsListSQL drop OFFSET and page by keyset instead — ORDER BY now always tie-breaks on id, and Query gains After *Cursor (built from the previous page's last Row via Row.Cursor); a client walks pages by feeding each page's last row's cursor into the next Query, which visits every row exactly once no matter how many rows tie on the order column, a guarantee OFFSET pagination never had; the gateway's lists take `after` (a row's opaque `cursor`) in place of `offset`, which they no longer accept.
---

# ADR 0014 — list reads page by cursor with an id tie-breaker

## Context

`QueryEntities` and `QueryRecords` paged with `ORDER BY <col> LIMIT
<n> OFFSET <m>` (`docsListSQL` in `query.go`, `tableListSQL` in
`table.go`). `<col>` defaults to `updated_at` and is not unique: many
rows can share the same value, most easily right after a bulk write that
touches them all in the same instant (a backfill, a migration, a batch
import) or simply by landing in the same wall-clock millisecond under
load.

`OFFSET` only counts rows past the start of the result set; it does not
remember which rows a previous page already returned. When two
separately-planned queries (page 1 and page 2) order a tied column, the
server is free to break the tie differently each time — nothing commits
it to one order across two queries, only within a single query's plan.
A row that lands on the losing side of that coin flip twice is skipped
forever; one that lands on the winning side twice is returned twice. An
incident on 2026-10-17 traced exactly this: an admin list view's pages,
read with `OFFSET`, disagreed with the total row count after a bulk
update left a block of rows with identical `updated_at` values straddling
a page boundary — some never appeared on any page, others appeared on
two.

## Decision

- **`ORDER BY` always tie-breaks on `id`.** Both builders now emit
  `ORDER BY <col> <dir>[ NULLS LAST], id <dir>` — `id` is the primary key
  of every table either path reads (`loom_t_<service>_<entity>`,
  `loom_entities`, `loom_records`), so pairing it with `<col>` makes the
  order total: two rows are never interchangeable, tied or not.
- **`Query.Offset` is gone; `Query.After *Cursor` replaces it.** A
  `Cursor` is `{Value string, Null bool, ID string}` — the order
  column's value (as text, bound the same untyped way a `Filter`'s value
  already is, so the column's own type drives the comparison) and `id`
  of a page's boundary row. `Row` gained a `Cursor(orderBy string)
  (Cursor, error)` method that builds one from a returned row for the
  same `OrderBy` the page was read with (`""` and `"updated_at"` both
  mean the `updated_at` column; a custom field reads its value out of
  `Row.Data`, so it needs a `Fields` that did not narrow the field out).
  Setting `q.After` to the previous page's last row's cursor and
  re-running the same query (same `Namespace`/`Filters`/`OrderBy`) reads
  the next page.
- **The keyset `WHERE` predicate (`keysetFilter`, `query.go`) replaces
  `OFFSET` entirely — no list read emits `OFFSET` any more.** For a
  non-null cursor value it is
  `(<col> IS NULL OR <col> <cmp> $v OR (<col> = $v AND id <cmp> $id))`,
  `<cmp>` being `<` for a descending order and `>` for ascending; the
  `<col> IS NULL` arm keeps every page landing the NULLS-LAST rows after
  every non-null one, matching the `ORDER BY` this pairs with. A cursor
  whose own value was `NULL` (`Cursor.Null`) compares only `id`, since
  it is already within the trailing NULL group. This predicate names the
  exact row set that sorts strictly after the cursor in the same total
  order the query's own `ORDER BY` produces, so it can never skip or
  repeat a row regardless of how many rows tie — proven by
  `TestQueryEntitiesCursorPagesEveryRowOnce` (`internal/e2e/pagination_test.go`):
  250 rows forced to one identical `updated_at`, read back over three
  `Limit=100` pages by feeding each page's last cursor into the next,
  union exactly the 250 ids, each exactly once.
- **The HTTP list endpoints (`GET /entities/{name}`, `GET
  /records/{name}`) take `after_id` (plus `after_value`, or
  `after_null=1` for a NULL-valued boundary) instead of `offset`**
  (`queryFromURL`, `api.go`) — the same three fields a `Cursor` carries,
  since URL query params have no natural way to carry a struct.
- **The gateway's lists take `after` and each row carries its
  `cursor`** (`listArgs`, `queryFromArgs`, `rowDocs` in
  `graphql/graphql.go`). `after: String` is the `cursor` of the last row
  of the previous page of the same list, with the same order and filters.
  A cursor is opaque: the order column's value, whether it was null, and
  the id, encoded. Every row type has a nullable `cursor` field, set on
  the rows of a list for the order it was read in and null on a single
  read. A narrowed read keeps the order column among the fields it
  fetches, since the cursor is built from it. `offset` is gone from the
  list arguments: a query that passes it is refused by the schema, where
  silently answering page 1 would have looked like a short list.
  Series lists, which are ordered by time and ranged with `since` and
  `until`, keep `offset`.

## Consequences

- A list read page is read exactly once per row across however many
  pages a caller walks, even when the order column ties across a page
  boundary — the OFFSET-era failure mode this ADR fixes is now
  structurally impossible, not just less likely.
- `tableListSQL`'s and `docsListSQL`'s unit tests
  (`table_unit_test.go`) assert the new shape directly: no `OFFSET` in
  any emitted SQL, `ORDER BY` always carrying the `id` tie-break, and the
  keyset predicate appearing only once `Query.After` is set.
- A caller that only ever reads page 1 (`After` nil) sees no behavior
  change beyond the `ORDER BY` clause itself now naming `id` — the
  `LIMIT` and row contents are identical to before.
- A GraphQL query that passes `offset` to a list no longer validates:
  the client moves to `after`. Queries that only pass `limit` are
  unaffected. The emitted SDL and OpenAPI say the same.
- A caller building its own `Cursor` for a custom `OrderBy` field needs
  that field in the page's `Fields` (or `Fields` left nil) — `Row.Cursor`
  reads the value out of `Row.Data`, which a narrowed read that left the
  field out does not carry; it returns an error rather than a wrong
  cursor in that case.
