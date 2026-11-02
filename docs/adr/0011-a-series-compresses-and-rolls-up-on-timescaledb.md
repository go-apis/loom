---
title: "ADR 0011 — a series compresses and rolls up on TimescaleDB"
status: proposed
date: 2026-11-02
summary: Two series directives that act only on TimescaleDB and do nothing on plain Postgres. @compress(age) turns on compression segmented by (service, namespace) and ordered by the dims then time, plus a compression policy Migrate keeps at the declared age. @rollup(hour, day) keeps one real-time continuous aggregate per unit, grouped by every dim, holding the parts that recombine exactly. QuerySeriesBuckets reads the coarsest rollup that gives the raw table's exact answer, and the raw table otherwise. A rollup is rebuildable, so Migrate replaces one whose definition changed and drops one no longer declared. Last now breaks ties on the dims on both paths.
---

# ADR 0011 — a series compresses and rolls up on TimescaleDB

## Context

optrader is moving its price data onto Tiger (managed TimescaleDB), with
loom as the only way in: records for decisions (product links, match
reviews, overrides) and `series` for observations (prices, sales, crawl
health). Loom already turns a series table into a hypertable when the
extension is present, and gives it a retention policy for `@retain`.
Two things are missing at optrader's volumes, and `TODO.md` has listed
both as series follow-ons since series landed:

- **Storage.** Nothing compresses. Price history from price-sync runs to
  tens of millions of rows; baseball alone is 1.5M products. On Tiger,
  storage is the bill, and TimescaleDB's columnstore usually shrinks this
  kind of data by 90% or more.
- **Reads.** `QuerySeriesBuckets` runs `date_trunc` over the raw rows
  every time (DESIGN.md keeps it engine-portable). A product page asking
  for a year of monthly prices, or a set summary over thousands of
  products, scans every row in the range.

Both must keep the declaration engine-neutral. A series that says
`@compress` or `@rollup` has to run unchanged on Cloud SQL, which has no
TimescaleDB.

## Decision

### `@compress(age)`

`@compress(30d)` takes the same duration forms as `@retain` (`d`, `w`,
`h` rounded up to days). On TimescaleDB, Migrate:

- enables compression on the hypertable once, with
  `compress_segmentby = 'service, namespace'` and
  `compress_orderby = '<dims…>, <time> DESC'`;
- adds a compression policy at the declared age, and when the declared
  age differs from the policy's (`@compress` was edited), moves the
  policy with `alter_job`. `if_not_exists` alone would keep the old age
  without saying so.

On plain Postgres it does nothing. Validation refuses a `@compress` age
at or beyond `@retain`, since a chunk would be dropped before it was
compressed.

**Why segment by namespace and not by the dims.** TimescaleDB's usual
advice is to segment by the column you filter on, but a series' dims
are its identity. A daily price series has about seven rows per product
per seven-day chunk, far too few per segment to compress. Ordering by
the dims instead keeps one product's rows together inside each
compressed batch of about 1,000 rows, and the batch's min/max metadata
on the first dim lets a filtered read skip batches.

### `@rollup(unit, …)`

The units are `hour` and `day`: fixed widths (`time_bucket` needs one)
that nest exactly in every coarser `date_trunc` unit the bucket query
serves. On TimescaleDB, Migrate keeps one continuous aggregate per
declared unit, `loom_r_<service>_<series>_<unit>`. Each:

- groups by `service, namespace, time_bucket(unit), <every dim>`;
- holds `loom_n` (count), `loom_first_at` and `loom_last_at`, and, for
  each numeric column that is not a dim, `<col>__sum`, `__n`, `__min`,
  `__max` and `__last`. These parts recombine exactly into any coarser
  bucket and any subset of the dims: count is a sum of counts, avg is a
  sum of sums over a sum of counts, min and max are min and max, and
  last is the part with the newest `loom_last_at`;
- is real-time (`materialized_only = false`), so rows not yet
  materialized are read from the raw table and an answer never lags an
  append;
- is created `WITH NO DATA`, with a refresh policy of
  `start_offset => NULL`, `end_offset` one unit and an hourly schedule.
  A NULL start re-materializes late rows wherever they land, through
  TimescaleDB's invalidation log, which price-sync's history backfill
  needs. A trailing window would leave old history unrolled.

A rollup can be rebuilt from the raw table, unlike the series itself.
Migrate keeps a hash of the definition in the view's comment, replaces a
view whose hash differs (a column was added, a dim changed), and drops a
view whose unit is no longer declared. On plain Postgres it does
nothing, and every bucket query reads the raw table.

### Reading

`QuerySeriesBuckets` reads a rollup only when the answer is exactly
the raw table's:

- the rollup's view exists (looked up once per client; Migrate makes
  the next query look again);
- the session time zone is UTC (`time_bucket` is UTC, and the raw path's
  `date_trunc` follows the session);
- the unit nests in the asked bucket, and `Since`/`Until` fall on the
  unit's boundaries;
- there are no percentiles (a rollup keeps no distribution);
- every filter names a dim (a rollup keeps no other column);
- the value is a numeric non-dim column;
- the series has no `@key`, because its dims would not identify a row
  and two rows of one group could share an instant.

Of the rollups that qualify, it reads the coarsest. Otherwise it reads
the raw table, as before. `SeriesBucketQuery.Raw` forces the raw read,
for proving the two agree.

**Last breaks ties on the dims, on both paths.** Raw `Last` was the
value of an arbitrary row among those sharing the newest instant, for
example four products' prices stamped by one crawl at the same second.
It is now the newest row, ties broken by the dims in declared order,
read the same way from a rollup. This is the one change to an existing
answer, and it only fixes what was arbitrary.

## Consequences

- One declaration, two engines: `series PricePoint … @compress(30d)
  @rollup(day)` runs on Cloud SQL as before and is cheaper and faster
  on Tiger.
- `TestSeriesCompressAndRollup` (e2e, skips without timescaledb) proves
  the following:
  - compression and its policy are created;
  - every bucket shape a rollup can answer equals the raw answer, both
    real-time and materialized;
  - a rollup is what answers;
  - appends into compressed chunks still converge;
  - Migrate is idempotent, replaces a changed rollup and moves a changed
    compression age.
- **Appending into compressed chunks costs more.** `ON CONFLICT DO
  NOTHING` into a compressed chunk decompresses the batches that could
  conflict. A history backfill that writes years-old rows into
  compressed chunks pays that on every batch. The test proves it is
  correct, not that it is fast. Before turning `@compress` on for a
  series a crawler backfills, measure a sample on Tiger, or set the
  compression age past the backfill's reach.
- A changed rollup is rebuilt in the background by its policy. Until
  then the real-time view computes from the raw table: correct, and as
  slow as no rollup.
- The views need `SELECT` for the serving role. Default privileges on
  tables cover views, so a deployment that grants that way needs
  nothing new.
- Removing `@compress` leaves compression and its policy in place.
  Decompressing is a hand step, like any other series change that
  touches data.
- Not done: chunk-interval control (`@chunk`), compressing the rollups
  themselves, and percentiles from rollups (would need the
  timescaledb_toolkit `percentile_agg`).
