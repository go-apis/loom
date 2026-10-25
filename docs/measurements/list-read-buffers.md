---
summary: EXPLAIN (ANALYZE, BUFFERS) buffer counts for an asked-fields @table list read vs the whole-row read, on 5000 TOASTed rows — asked/whole ratio 0.0060.
---

# Measurement: list-read buffers, asked fields vs whole row

Command:

```sh
go run ./cmd/explainlist
```

Date: 2026-10-25
Postgres: `PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
(the project's declared test service, 127.0.0.1:5432)

`cmd/explainlist` (PR #90) builds a scratch `@table` mirror shaped like a
loom read model — `service`, `namespace`, `id`, `updated_at`, plus `status`,
`total_cents` and an 8192-byte `heavy` text column big enough that Postgres
stores it out-of-line (TOAST) — fills it with 5000 rows, and runs
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` over the exact SQL `table.go`'s
`tableListSQL` builds for a page of 50: once with `Query.Fields` nil (the
whole-row default) and once with `Query.Fields = []string{"status"}` (the
opt-in asked-fields read, ADR 0009).

Output, verbatim:

```
whole-row page   (Fields nil):       shared hit=60360 read=0 total=60360
asked-fields page (Fields=[status]):  shared hit=360 read=0 total=360
ratio (asked/whole): 0.0060
```

The asked-fields page's shared buffers are under a hundredth of the
whole-row page's — well inside the goal's "under a tenth" bar.

## Caveats

- All buffers came back as shared **hits** (`read=0`): the scratch table
  was freshly `ANALYZE`d and small enough to sit in shared_buffers for this
  run, so the ratio reflects pages *touched*, not disk I/O. The gap would
  only widen (more TOAST pages to fetch from disk) on a table too large to
  cache.
- Postgres 16's `EXPLAIN ANALYZE` has no `SERIALIZE` option (that lands in
  17), so it does not account for client-side detoasting of the output
  rows. The whole-row page's extra buffers come from Postgres reading the
  TOASTed `heavy` column's out-of-line chunks to build `to_jsonb(t)`, not
  from a step this EXPLAIN form can't see — the buffer counts above are the
  server-side read work, which is what this measurement is after.
- This run exercises the `@table` path (`tableListSQL`) only. The
  jsonb-document path (`docsListSQL`, entities without `@table`) narrows
  the *output* to the asked fields the same way, but still reads and
  detoasts the whole `data` column to do it — its saving is on the wire
  and in JSON decoding, not in buffers read. See ADR 0009.
