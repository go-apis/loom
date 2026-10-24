---
title: "EXPLAIN buffers: a page read of asked fields vs. the whole row"
date: 2026-10-24
summary: On a @table entity with an 8KB TOASTed `heavy` column and 5000 rows, a 50-row asked-fields page (Fields=[status]) read 360 shared buffers against the whole-row page's 60360 — a ratio of 0.0060, well under a tenth.
---

# EXPLAIN buffers: a page read of asked fields vs. the whole row

`go run ./cmd/explainlist` (`cmd/explainlist/main.go`) measures what ADR
0009's asked-fields read saves over the whole-row read it defaults to.

## Setup

- A throwaway table, `loom_explainlist_heavy`, shaped like a `@table`
  mirror: `service`, `namespace`, `id`, `updated_at` (meta columns) plus
  `status` (text), `total_cents` (bigint) and `heavy` (text, ~8192
  random alphanumeric characters per row — well past Postgres's ~2KB
  inline threshold, so every row's `heavy` value is stored TOASTed,
  out-of-line).
- 5000 rows, one `service`/`namespace` pair, inserted via `COPY`.
- `ANALYZE loom_explainlist_heavy` after the insert.
- Two queries, byte-for-byte the SQL shape `tableListSQL` (`table.go`)
  builds for a 50-row page, the only difference being `Query.Fields`:
  - **whole-row** (`Fields` nil): `SELECT id, namespace, updated_at,
    to_jsonb(t) - 'service' - 'namespace' - 'id' - 'updated_at' FROM
    loom_explainlist_heavy t WHERE service=$1 AND namespace=$2 ORDER BY
    updated_at DESC NULLS LAST LIMIT 50 OFFSET 0`
  - **asked-fields** (`Fields=["status"]`): the same query with the
    document expression replaced by `jsonb_build_object('status',
    "status")`
- Each ran once under `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`, and the
  script sums `Shared Hit Blocks` + `Shared Read Blocks` over every node
  of the returned plan tree (Postgres reports each node's own buffer
  counts, not a running total).

Environment: PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2), the repository's
docker-compose Postgres service, warm cache (no `Shared Read Blocks` on
either run — the whole table fit in `shared_buffers` after `ANALYZE`, so
every buffer counted was a hit, not a disk read).

## Result

Run on 2026-10-24, `go run ./cmd/explainlist`:

```
whole-row page  (Fields nil):        shared hit=60360 read=0 total=60360
asked-fields page (Fields=[status]):  shared hit=360 read=0 total=360
ratio (asked/whole): 0.0060
```

The asked-fields page read **0.60%** of the whole-row page's shared
buffers — well under the goal's "under a tenth" bar. The whole-row read's
60360 buffers are almost entirely TOAST detoasting: `to_jsonb(t)` forces
every one of the 50 returned rows' `heavy` values off the TOAST table
(an ~8KB value chunked across several TOAST rows, each a separate
buffer), while the asked-fields read never touches `heavy` or its TOAST
table at all — its buffers are the base-table page reads needed to
evaluate the `WHERE`/`ORDER BY` over all 5000 rows, which `Fields` does
not change (narrowing `Fields` narrows the projected document, not the
scan).

## Caveat

This measurement is for the `@table` path. The jsonb-entity path's `data`
column (one jsonb value per row) is still detoasted whole regardless of
`Fields` — `jsonb_build_object('f', data->'f')` applies `->` only after
Postgres has the full value in memory, so its saving from `Fields` is on
the wire and in decoding, not in buffers. See ADR 0009.
