package loom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// TimescaleDB-only series upkeep (ADR 0011): @compress and @rollup.
// Both are no-ops on plain Postgres, so a declaration stays
// engine-neutral — a schema that says @compress(30d) @rollup(day) runs
// on Cloud SQL unchanged, reading every bucket from the raw table.
//
// Compression is segmented by (service, namespace) and ordered by the
// dims then time, newest first. Segmenting by the dims themselves is
// TimescaleDB's usual advice, but a series' dims are its identity — a
// product id — and a day-per-row series has a handful of rows per dim
// per chunk, too few to compress. Ordering by the dims instead keeps
// one dim's rows together inside each compressed batch, and the batch's
// min/max metadata on the first dim lets a filtered read skip batches.
//
// A rollup is a continuous aggregate per declared unit, grouped by
// (service, namespace, every dim) with count, first/last time and, per
// numeric non-dim column, sum, count, min, max and last. Those are the
// parts QuerySeriesBuckets' answer recombines exactly into any coarser
// bucket and any subset of the dims. It is real-time
// (materialized_only = false), so rows not yet materialized are read
// from the raw table and the answer never lags an append. A rollup is
// rebuildable from the raw table, unlike the series itself, so Migrate
// replaces one whose definition changed and drops one no longer
// declared.

// seriesRollupWidth is the time_bucket width of each @rollup unit.
var seriesRollupWidth = map[string]string{"hour": "1 hour", "day": "1 day"}

// seriesUnitRank orders date_trunc units by width, for "does a rollup
// of unit r nest in a bucket of unit q": r's rank ≤ q's.
var seriesUnitRank = map[string]int{"hour": 0, "day": 1, "week": 2, "month": 3, "year": 4, "all": 5}

// seriesRollupMeta prefixes the rollup's own columns so none can collide
// with a dim; per-column parts are <column>__<part>.
const (
	rollupBucket  = "loom_bt"
	rollupCount   = "loom_n"
	rollupFirstAt = "loom_first_at"
	rollupLastAt  = "loom_last_at"
)

// rollupView names a series' continuous aggregate for one unit:
// loom_s_<service>_<series> becomes loom_r_<service>_<series>_<unit>.
func rollupView(def *SeriesDef, unit string) string {
	return "loom_r_" + strings.TrimPrefix(def.Table, "loom_s_") + "_" + unit
}

// rollupValues are the numeric columns a rollup aggregates: every
// bigint or double precision column that is neither the time nor a dim.
func rollupValues(def *SeriesDef) []string {
	var out []string
	for _, col := range def.Columns {
		if col.Name == def.Time || slices.Contains(def.Dims, col.Name) {
			continue
		}
		switch col.Type {
		case "bigint", "double precision":
			out = append(out, col.Name)
		}
	}
	return out
}

// rollupSQL is the continuous aggregate's defining query.
func rollupSQL(def *SeriesDef, unit string) string {
	t := quoteIdent(def.Time)
	group := []string{"service", "namespace", rollupBucket}
	sel := []string{"service", "namespace",
		fmt.Sprintf("time_bucket(INTERVAL '%s', %s) AS %s", seriesRollupWidth[unit], t, rollupBucket)}
	for _, d := range def.Dims {
		sel = append(sel, quoteIdent(d))
		group = append(group, quoteIdent(d))
	}
	sel = append(sel,
		"count(*) AS "+rollupCount,
		fmt.Sprintf("min(%s) AS %s", t, rollupFirstAt),
		fmt.Sprintf("max(%s) AS %s", t, rollupLastAt))
	for _, v := range rollupValues(def) {
		c := quoteIdent(v)
		sel = append(sel,
			fmt.Sprintf("sum(%s)::float8 AS %s", c, quoteIdent(v+"__sum")),
			fmt.Sprintf("count(%s) AS %s", c, quoteIdent(v+"__n")),
			fmt.Sprintf("min(%s)::float8 AS %s", c, quoteIdent(v+"__min")),
			fmt.Sprintf("max(%s)::float8 AS %s", c, quoteIdent(v+"__max")),
			fmt.Sprintf("last(%s, %s)::float8 AS %s", c, t, quoteIdent(v+"__last")))
	}
	return fmt.Sprintf("SELECT %s FROM %s GROUP BY %s",
		strings.Join(sel, ", "), def.Table, strings.Join(group, ", "))
}

// rollupHash fingerprints a rollup's definition; Migrate keeps it in the
// view's comment and replaces the view when it changes.
func rollupHash(def *SeriesDef, unit string) string {
	sum := sha256.Sum256([]byte(rollupSQL(def, unit)))
	return "loom:" + hex.EncodeToString(sum[:8])
}

// migrateSeriesTimescale applies @compress and @rollup to one series
// whose table is already a hypertable.
func (c *Client) migrateSeriesTimescale(ctx context.Context, def *SeriesDef) error {
	if def.CompressDays > 0 {
		if err := c.migrateSeriesCompression(ctx, def); err != nil {
			return err
		}
	}
	return c.migrateSeriesRollups(ctx, def)
}

func (c *Client) migrateSeriesCompression(ctx context.Context, def *SeriesDef) error {
	var enabled bool
	if err := c.db.QueryRow(ctx, `SELECT compression_enabled FROM timescaledb_information.hypertables
		WHERE hypertable_schema = current_schema() AND hypertable_name = $1`, def.Table).Scan(&enabled); err != nil {
		return fmt.Errorf("loom: compression state %s: %w", def.Table, err)
	}
	if !enabled {
		order := make([]string, 0, len(def.Dims)+1)
		for _, d := range def.Dims {
			order = append(order, quoteIdent(d))
		}
		order = append(order, quoteIdent(def.Time)+" DESC")
		if _, err := c.db.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = 'service, namespace', timescaledb.compress_orderby = '%s')`,
			def.Table, strings.ReplaceAll(strings.Join(order, ", "), "'", "''"))); err != nil {
			return fmt.Errorf("loom: compress %s: %w", def.Table, err)
		}
	}
	after := fmt.Sprintf("%d days", def.CompressDays)
	// A changed @compress age moves the existing policy rather than
	// leaving the old one in force (add_compression_policy's
	// if_not_exists would keep it).
	if _, err := c.db.Exec(ctx, `SELECT alter_job(j.job_id, config => j.config || jsonb_build_object('compress_after', $2::text))
		FROM timescaledb_information.jobs j
		WHERE j.proc_name = 'policy_compression' AND j.hypertable_schema = current_schema() AND j.hypertable_name = $1
		  AND (j.config->>'compress_after')::interval <> $2::interval`, def.Table, after); err != nil {
		return fmt.Errorf("loom: compression policy %s: %w", def.Table, err)
	}
	if _, err := c.db.Exec(ctx,
		`SELECT add_compression_policy($1::regclass, ($2::text)::interval, if_not_exists => TRUE)`,
		def.Table, after); err != nil {
		return fmt.Errorf("loom: compression policy %s: %w", def.Table, err)
	}
	return nil
}

func (c *Client) migrateSeriesRollups(ctx context.Context, def *SeriesDef) error {
	for unit := range seriesRollupWidth {
		view := rollupView(def, unit)
		if len(view) > 63 {
			if slices.Contains(def.Rollups, unit) {
				return fmt.Errorf("loom: rollup %s is longer than 63 characters — shorten the series or service name", view)
			}
			continue
		}
		var comment *string
		if err := c.db.QueryRow(ctx, `SELECT obj_description(to_regclass($1), 'pg_class')`, view).Scan(&comment); err != nil {
			return fmt.Errorf("loom: rollup %s: %w", view, err)
		}
		var exists bool
		if err := c.db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, view).Scan(&exists); err != nil {
			return fmt.Errorf("loom: rollup %s: %w", view, err)
		}
		if !slices.Contains(def.Rollups, unit) {
			if exists {
				if _, err := c.db.Exec(ctx, `DROP MATERIALIZED VIEW `+view); err != nil {
					return fmt.Errorf("loom: drop rollup %s: %w", view, err)
				}
			}
			continue
		}
		hash := rollupHash(def, unit)
		if exists && (comment == nil || *comment != hash) {
			if _, err := c.db.Exec(ctx, `DROP MATERIALIZED VIEW `+view); err != nil {
				return fmt.Errorf("loom: replace rollup %s: %w", view, err)
			}
			exists = false
		}
		if !exists {
			// WITH NO DATA: the refresh policy materializes in the
			// background; until it has, the real-time view answers from
			// the raw table, correct and only as slow as before.
			if _, err := c.db.Exec(ctx, fmt.Sprintf(
				`CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS %s WITH NO DATA`,
				view, rollupSQL(def, unit))); err != nil {
				return fmt.Errorf("loom: create rollup %s: %w", view, err)
			}
			if _, err := c.db.Exec(ctx, fmt.Sprintf(`COMMENT ON VIEW %s IS '%s'`, view, hash)); err != nil {
				return fmt.Errorf("loom: rollup %s: %w", view, err)
			}
		}
		// start_offset NULL: late rows (a crawler's history backfill) are
		// re-materialized wherever they land, through the invalidation
		// log, rather than only inside a trailing window.
		width := seriesRollupWidth[unit]
		if _, err := c.db.Exec(ctx, `SELECT add_continuous_aggregate_policy($1::regclass,
			start_offset => NULL, end_offset => ($2::text)::interval, schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE)`,
			view, width); err != nil {
			return fmt.Errorf("loom: rollup policy %s: %w", view, err)
		}
	}
	return nil
}

// seriesRollupState is what the serving process learned about rollups,
// looked up on the first bucket query and kept: which views exist and
// whether the session's time zone is UTC (time_bucket is UTC; the raw
// path's date_trunc follows the session, so a rollup answers exactly
// only on a UTC session). A failed lookup is tried again next query.
type seriesRollupState struct {
	mu     sync.Mutex
	loaded bool
	utc    bool
	views  map[string]bool
}

func (c *Client) rollupViews(ctx context.Context) (map[string]bool, error) {
	st := &c.seriesRollups
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.loaded {
		if !st.utc {
			return nil, nil
		}
		return st.views, nil
	}
	var tz string
	if err := c.db.QueryRow(ctx, `SELECT current_setting('TimeZone')`).Scan(&tz); err != nil {
		return nil, err
	}
	views := map[string]bool{}
	for _, ss := range c.series {
		for _, unit := range ss.def.Rollups {
			view := rollupView(ss.def, unit)
			var ok bool
			if err := c.db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, view).Scan(&ok); err != nil {
				return nil, err
			}
			views[view] = ok
		}
	}
	switch strings.ToLower(tz) {
	case "utc", "etc/utc", "gmt", "etc/gmt", "universal", "zulu", "etc/universal", "etc/zulu":
		st.utc = true
	}
	st.views, st.loaded = views, true
	if !st.utc {
		return nil, nil
	}
	return views, nil
}

// forgetRollups drops what rollupViews learned, so the next bucket query
// looks again — Migrate calls it after creating or dropping a view.
func (c *Client) forgetRollups() {
	c.seriesRollups.mu.Lock()
	c.seriesRollups.loaded = false
	c.seriesRollups.mu.Unlock()
}

// pickRollup returns the rollup view that answers q exactly, or "" for
// the raw table: the coarsest declared unit that exists, nests in the
// bucket, and starts and ends the range on its own boundaries, for a
// query with no percentiles (a rollup keeps no distribution) whose
// filters name only dims (a rollup keeps no other column), on a series
// whose dims identify a row (no @key).
func pickRollup(def *SeriesDef, q SeriesBucketQuery, views map[string]bool) string {
	// A @key series' dims do not identify a row, so two rows of one dim
	// group can share an instant and a rollup's last() could pick either.
	if q.Raw || len(q.Percentiles) > 0 || len(def.Keys) > 0 || !slices.Contains(rollupValues(def), q.Value) {
		return ""
	}
	for _, f := range q.Filters {
		if !slices.Contains(def.Dims, f.Field) {
			return ""
		}
	}
	best, bestRank := "", -1
	for _, unit := range def.Rollups {
		rank, ok := seriesUnitRank[unit]
		if !ok || rank > seriesUnitRank[q.Bucket] || rank <= bestRank {
			continue
		}
		if !views[rollupView(def, unit)] || !onBoundary(q.Since, unit) || !onBoundary(q.Until, unit) {
			continue
		}
		best, bestRank = rollupView(def, unit), rank
	}
	return best
}

func onBoundary(t time.Time, unit string) bool {
	if t.IsZero() {
		return true
	}
	u := t.UTC()
	switch unit {
	case "hour":
		return u.Equal(u.Truncate(time.Hour))
	case "day":
		return u.Equal(time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC))
	}
	return false
}

// rollupBucketSelect is the select list of a bucket query read from a
// rollup: the same columns, in the same order, the raw query returns,
// with Last tie-broken the raw query's way (newest, then the dims).
func rollupBucketSelect(def *SeriesDef, q SeriesBucketQuery) string {
	var b strings.Builder
	if q.Bucket == "all" {
		fmt.Fprintf(&b, "SELECT min(%s) AS bt", rollupFirstAt)
	} else {
		fmt.Fprintf(&b, "SELECT date_trunc('%s', %s) AS bt", q.Bucket, rollupBucket)
	}
	for _, by := range q.By {
		fmt.Fprintf(&b, ", %s::text", quoteIdent(by))
	}
	v := func(part string) string { return quoteIdent(q.Value + "__" + part) }
	order := rollupLastAt + " DESC"
	for _, d := range def.Dims {
		order += ", " + quoteIdent(d)
	}
	fmt.Fprintf(&b, ", sum(%s)::bigint, (sum(%s) / nullif(sum(%s), 0))::float8, min(%s)::float8, max(%s)::float8, (array_agg(%s ORDER BY %s))[1]::float8",
		rollupCount, v("sum"), v("n"), v("min"), v("max"), v("last"), order)
	return b.String()
}
