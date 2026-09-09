package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Series: append-only time-series observations in typed per-series
// tables. Rows never touch the log, the outbox, or the global sequence —
// they are facts about the outside world, not domain decisions, at
// volumes the event store must not carry. Like @table, the generated
// SeriesDef owns the shape and everything here is built once at boot.
//
// Unlike read models, series data is NOT rebuildable: the table is the
// only copy. That is why migrateSeries never drops or alters anything
// and reports incompatible drift for hand migration.

type seriesSQL struct {
	def      *SeriesDef
	conflict string // ON CONFLICT (identity...) DO NOTHING
	colType  map[string]string
}

func buildSeries(reg *Registry) map[string]*seriesSQL {
	out := map[string]*seriesSQL{}
	for _, def := range reg.Series {
		colType := map[string]string{}
		for _, col := range def.Columns {
			colType[col.Name] = col.Type
		}
		ident := []string{"service", "namespace"}
		for _, f := range def.IdentityColumns() {
			ident = append(ident, quoteIdent(f))
		}
		ident = append(ident, quoteIdent(def.Time))
		out[def.Name] = &seriesSQL{
			def:      def,
			conflict: fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(ident, ", ")),
			colType:  colType,
		}
	}
	return out
}

// migrateSeries applies each series' DDL, then adapts the table to the
// engine: with the timescaledb extension installed it becomes a
// hypertable on the time column, otherwise a BRIN time index makes
// range scans cheap on plain Postgres — the declaration is
// engine-neutral, per the same escalation path the event log documents.
// The column diff is additive-only, like @table; incompatible drift is
// an error the operator resolves by hand, because series data has no
// rebuild source.
func (c *Client) migrateSeries(ctx context.Context) error {
	if len(c.series) == 0 {
		return nil
	}
	var timescale bool
	if err := c.db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')`).Scan(&timescale); err != nil {
		return err
	}
	c.seriesTimescale = timescale
	var drift []string
	for _, ss := range sortedSeries(c.series) {
		def := ss.def
		retained := def.RetainDays > 0 && def.PartitionDDL != ""
		if retained && !timescale {
			// A @retain series on plain Postgres is range-partitioned by
			// day so expiry is DROP TABLE. An existing unpartitioned table
			// cannot be converted in place and its data has no rebuild
			// source, so that is drift for a hand migration.
			exists, partitioned, err := c.tableShape(ctx, def.Table)
			if err != nil {
				return err
			}
			if exists && !partitioned {
				drift = append(drift, fmt.Sprintf("%s exists unpartitioned but the series is @retain (create the partitioned table and move the rows)", def.Table))
				continue
			}
			if _, err := c.db.Exec(ctx, def.PartitionDDL); err != nil {
				return fmt.Errorf("loom: create %s: %w", def.Table, err)
			}
		} else {
			if _, err := c.db.Exec(ctx, def.DDL); err != nil {
				return fmt.Errorf("loom: create %s: %w", def.Table, err)
			}
		}
		if timescale {
			if _, err := c.db.Exec(ctx,
				`SELECT create_hypertable($1::regclass, $2::name, if_not_exists => TRUE, migrate_data => TRUE)`,
				def.Table, def.Time); err != nil {
				return fmt.Errorf("loom: hypertable %s: %w", def.Table, err)
			}
			if def.RetainDays > 0 {
				if _, err := c.db.Exec(ctx,
					`SELECT add_retention_policy($1::regclass, ($2::text)::interval, if_not_exists => TRUE)`,
					def.Table, fmt.Sprintf("%d days", def.RetainDays)); err != nil {
					return fmt.Errorf("loom: retention policy %s: %w", def.Table, err)
				}
			}
		} else {
			if _, err := c.db.Exec(ctx, fmt.Sprintf(
				`CREATE INDEX IF NOT EXISTS %s_brin ON %s USING brin (%s)`,
				def.Table, def.Table, quoteIdent(def.Time))); err != nil {
				return fmt.Errorf("loom: index %s: %w", def.Table, err)
			}
			if retained {
				if err := c.maintainSeriesPartitions(ctx, def); err != nil {
					return err
				}
			}
		}
		live, err := c.liveColumns(ctx, def.Table)
		if err != nil {
			return err
		}
		for _, col := range def.Columns {
			liveType, ok := live[col.Name]
			if !ok {
				if _, err := c.db.Exec(ctx, fmt.Sprintf(
					`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s`,
					def.Table, quoteIdent(col.Name), col.Type)); err != nil {
					return fmt.Errorf("loom: add %s.%s: %w", def.Table, col.Name, err)
				}
				continue
			}
			if liveType != pgTypeName(col.Type) {
				drift = append(drift, fmt.Sprintf(
					"%s.%s is %s, schema wants %s", def.Table, col.Name, liveType, col.Type))
			}
		}
	}
	if len(drift) > 0 {
		return fmt.Errorf(
			"loom: incompatible series drift (series data is NOT rebuildable — migrate the column by hand):\n  - %s",
			strings.Join(drift, "\n  - "))
	}
	return nil
}

// tableShape reports whether a table exists and whether it is a
// declaratively partitioned parent.
func (c *Client) tableShape(ctx context.Context, table string) (exists, partitioned bool, err error) {
	err = c.db.QueryRow(ctx, `
		SELECT count(*) > 0, coalesce(bool_or(c.relkind = 'p'), false)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND n.nspname = current_schema() AND c.relkind IN ('r', 'p')`, table).Scan(&exists, &partitioned)
	return exists, partitioned, err
}

// seriesPartitionAhead is how many days of partitions maintenance keeps
// created in advance, so a stalled runner does not stall appends (an
// append into a missing day repairs itself too).
const seriesPartitionAhead = 7

// MaintainSeries does the periodic upkeep every @retain series needs on
// plain Postgres: create the next days' partitions and drop the ones
// past retention. The runner calls it hourly; Migrate calls it once. It
// is a no-op on TimescaleDB, where the retention policy does the work.
func (c *Client) MaintainSeries(ctx context.Context) error {
	if c.seriesTimescale {
		return nil
	}
	// One replica at a time: CREATE/DROP of the same partition from two
	// instances can trip a catalog uniqueness race even with IF NOT
	// EXISTS. The lock is transaction-scoped and released at commit; a
	// replica that does not get it simply skips this tick.
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('loom_series_retain_' || $1))`, c.reg.Service).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return nil
	}
	for _, ss := range sortedSeries(c.series) {
		if ss.def.RetainDays > 0 && ss.def.PartitionDDL != "" {
			if err := c.maintainSeriesPartitions(ctx, ss.def); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (c *Client) maintainSeriesPartitions(ctx context.Context, def *SeriesDef) error {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for i := -1; i <= seriesPartitionAhead; i++ {
		day := today.AddDate(0, 0, i)
		part := seriesPartitionName(def.Table, day)
		if _, err := c.db.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
			part, def.Table, day.Format("2006-01-02"), day.AddDate(0, 0, 1).Format("2006-01-02"))); err != nil {
			return fmt.Errorf("loom: partition %s: %w", part, err)
		}
	}
	cutoff := today.AddDate(0, 0, -def.RetainDays)
	rows, err := c.db.Query(ctx, `
		SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = $1::regclass AND c.relname LIKE $2`, def.Table, def.Table+"_p________")
	if err != nil {
		return fmt.Errorf("loom: list partitions %s: %w", def.Table, err)
	}
	var expired []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		day, err := time.Parse("20060102", strings.TrimPrefix(name, def.Table+"_p"))
		if err == nil && day.Before(cutoff) {
			expired = append(expired, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range expired {
		if _, err := c.db.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name)); err != nil {
			return fmt.Errorf("loom: drop partition %s: %w", name, err)
		}
	}
	return nil
}

// ensureSeriesPartitionsFor creates the day partitions the rows need,
// skipping days past retention.
func (c *Client) ensureSeriesPartitionsFor(ctx context.Context, ss *seriesSQL, rows []SeriesRow) error {
	timeIdx := -1
	for i, col := range ss.def.Columns {
		if col.Name == ss.def.Time {
			timeIdx = i
			break
		}
	}
	if timeIdx < 0 {
		return nil
	}
	cutoff := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -ss.def.RetainDays)
	days := map[time.Time]bool{}
	for _, row := range rows {
		values := ss.def.Values(row)
		if timeIdx >= len(values) {
			continue
		}
		var at time.Time
		switch v := values[timeIdx].(type) {
		case time.Time:
			at = v
		case *time.Time:
			if v != nil {
				at = *v
			}
		}
		if at.IsZero() {
			continue
		}
		day := at.UTC().Truncate(24 * time.Hour)
		if !day.Before(cutoff) {
			days[day] = true
		}
	}
	for day := range days {
		part := seriesPartitionName(ss.def.Table, day)
		if _, err := c.db.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
			part, ss.def.Table, day.Format("2006-01-02"), day.AddDate(0, 0, 1).Format("2006-01-02"))); err != nil {
			return fmt.Errorf("loom: partition %s: %w", part, err)
		}
	}
	return nil
}

// seriesPartitionName is <table>_pYYYYMMDD. Postgres truncates identifiers
// past 63 bytes, so a long service+series name shortens here first to
// keep the day suffix intact.
func seriesPartitionName(table string, day time.Time) string {
	suffix := "_p" + day.Format("20060102")
	if len(table)+len(suffix) > 63 {
		table = table[:63-len(suffix)]
	}
	return table + suffix
}

func sortedSeries(series map[string]*seriesSQL) []*seriesSQL {
	out := make([]*seriesSQL, 0, len(series))
	for _, ss := range series {
		out = append(out, ss)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].def.Name < out[j].def.Name })
	return out
}

// seriesAppendChunk bounds one INSERT's VALUES list; 500 rows of a wide
// series stays well under Postgres's 65535-parameter cap.
const seriesAppendChunk = 500

// AppendSeries writes observations directly into their series tables —
// no unit of work, no dispatch, no events. Rows may mix series; each
// series' batch inserts with ON CONFLICT DO NOTHING on the identity
// columns, so re-appending a batch (a retried load, an overlapping
// crawl) converges. Returns how many rows were actually new.
func (c *Client) AppendSeries(ctx context.Context, namespace string, rows ...SeriesRow) (int64, error) {
	if namespace == "" {
		return 0, fmt.Errorf("loom: AppendSeries needs a namespace")
	}
	var order []string
	grouped := map[string][]SeriesRow{}
	for _, row := range rows {
		name := row.LoomSeries()
		if _, ok := grouped[name]; !ok {
			if c.series[name] == nil {
				return 0, fmt.Errorf("loom: unknown series %s", name)
			}
			order = append(order, name)
		}
		grouped[name] = append(grouped[name], row)
	}
	var inserted int64
	for _, name := range order {
		n, err := c.appendSeriesRows(ctx, c.series[name], namespace, grouped[name])
		inserted += n
		if err != nil {
			return inserted, err
		}
	}
	return inserted, nil
}

func (c *Client) appendSeriesRows(ctx context.Context, ss *seriesSQL, namespace string, rows []SeriesRow) (int64, error) {
	cols := []string{"service", "namespace"}
	for _, col := range ss.def.Columns {
		cols = append(cols, quoteIdent(col.Name))
	}
	var inserted int64
	for start := 0; start < len(rows); start += seriesAppendChunk {
		chunk := rows[start:min(start+seriesAppendChunk, len(rows))]
		var b strings.Builder
		fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES ", ss.def.Table, strings.Join(cols, ", "))
		args := make([]any, 0, len(chunk)*len(cols))
		for i, row := range chunk {
			values := ss.def.Values(row)
			for _, v := range values {
				if err, ok := v.(error); ok { // a JSONValue marshal failure
					return inserted, fmt.Errorf("loom: append %s: %w", ss.def.Name, err)
				}
			}
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("(")
			for j := 0; j < len(cols); j++ {
				if j > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, "$%d", len(args)+j+1)
			}
			b.WriteString(")")
			args = append(append(args, c.reg.Service, namespace), values...)
		}
		b.WriteString(ss.conflict)
		tag, err := c.db.Exec(ctx, b.String(), args...)
		if err != nil && isMissingPartition(err) && ss.def.RetainDays > 0 && !c.seriesTimescale {
			// A day in this chunk has no partition yet (maintenance has not
			// run since midnight, or the rows are a backfill): create the
			// partitions for the chunk's days that are inside retention
			// and retry once. Rows older than retention keep failing —
			// they would be dropped at the next sweep anyway.
			if merr := c.ensureSeriesPartitionsFor(ctx, ss, chunk); merr != nil {
				return inserted, merr
			}
			tag, err = c.db.Exec(ctx, b.String(), args...)
		}
		if err != nil {
			return inserted, fmt.Errorf("loom: append %s: %w", ss.def.Name, err)
		}
		inserted += tag.RowsAffected()
	}
	return inserted, nil
}

func isMissingPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && strings.Contains(pgErr.Message, "no partition of relation")
}

// RetractSeries deletes observations by identity — the mirror of
// AppendSeries for the rare fact that stops being true upstream (a
// source removes a sale, a correction re-buckets a row). Each row's
// identity columns and time select at most one stored row; its other
// columns are ignored. Rows may mix series. Returns how many stored
// rows actually went away, so retracting an already-absent row
// converges silently — the same idempotence contract as append.
func (c *Client) RetractSeries(ctx context.Context, namespace string, rows ...SeriesRow) (int64, error) {
	if namespace == "" {
		return 0, fmt.Errorf("loom: RetractSeries needs a namespace")
	}
	var order []string
	grouped := map[string][]SeriesRow{}
	for _, row := range rows {
		name := row.LoomSeries()
		if _, ok := grouped[name]; !ok {
			if c.series[name] == nil {
				return 0, fmt.Errorf("loom: unknown series %s", name)
			}
			order = append(order, name)
		}
		grouped[name] = append(grouped[name], row)
	}
	var deleted int64
	for _, name := range order {
		n, err := c.retractSeriesRows(ctx, c.series[name], namespace, grouped[name])
		deleted += n
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func (c *Client) retractSeriesRows(ctx context.Context, ss *seriesSQL, namespace string, rows []SeriesRow) (int64, error) {
	ident := append(append([]string{}, ss.def.IdentityColumns()...), ss.def.Time)
	pos := map[string]int{}
	for i, col := range ss.def.Columns {
		pos[col.Name] = i
	}
	quoted := make([]string, len(ident))
	for i, col := range ident {
		quoted[i] = quoteIdent(col)
	}
	var deleted int64
	for start := 0; start < len(rows); start += seriesAppendChunk {
		chunk := rows[start:min(start+seriesAppendChunk, len(rows))]
		var b strings.Builder
		fmt.Fprintf(&b, "DELETE FROM %s WHERE service=$1 AND namespace=$2 AND (%s) IN (",
			ss.def.Table, strings.Join(quoted, ", "))
		args := make([]any, 0, 2+len(chunk)*len(ident))
		args = append(args, c.reg.Service, namespace)
		for i, row := range chunk {
			values := ss.def.Values(row)
			for _, v := range values {
				if err, ok := v.(error); ok { // a JSONValue marshal failure
					return deleted, fmt.Errorf("loom: retract %s: %w", ss.def.Name, err)
				}
			}
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("(")
			for j := 0; j < len(ident); j++ {
				if j > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, "$%d", len(args)+j+1)
			}
			b.WriteString(")")
			for _, col := range ident {
				args = append(args, values[pos[col]])
			}
		}
		b.WriteString(")")
		tag, err := c.db.Exec(ctx, b.String(), args...)
		if err != nil {
			return deleted, fmt.Errorf("loom: retract %s: %w", ss.def.Name, err)
		}
		deleted += tag.RowsAffected()
	}
	return deleted, nil
}

// SeriesQuery filters one series by namespace, columns, and time range.
type SeriesQuery struct {
	Namespace string
	// AllNamespaces searches across every namespace — the god-mode read;
	// rows carry their namespace either way.
	AllNamespaces bool
	Filters       []Filter
	Since, Until  time.Time
	// Ascending orders oldest-first; the default is newest-first.
	Ascending bool
	Limit     int // default 1000, max 10000
	Offset    int
}

// SeriesPoint is one observation: its namespace plus the row as a doc.
type SeriesPoint struct {
	Namespace string          `json:"namespace"`
	Data      json.RawMessage `json:"data"`
}

// QuerySeries is the raw range read: filters compile to real columns,
// the time range rides the primary key (or the hypertable's chunks),
// rows come back as docs keyed by field name.
func (c *Client) QuerySeries(ctx context.Context, series string, q SeriesQuery) ([]SeriesPoint, error) {
	ss := c.series[series]
	if ss == nil {
		return nil, fmt.Errorf("loom: unknown series %s", series)
	}
	if q.Namespace == "" && !q.AllNamespaces {
		return nil, fmt.Errorf("loom: query needs a namespace")
	}
	if q.Limit <= 0 {
		q.Limit = 1000
	}
	if q.Limit > 10000 {
		q.Limit = 10000
	}

	var b strings.Builder
	var args []any
	if q.AllNamespaces {
		args = []any{c.reg.Service}
		fmt.Fprintf(&b, `SELECT namespace, to_jsonb(t) - 'service' - 'namespace' FROM %s t WHERE service=$1`, ss.def.Table)
	} else {
		args = []any{c.reg.Service, q.Namespace}
		fmt.Fprintf(&b, `SELECT namespace, to_jsonb(t) - 'service' - 'namespace' FROM %s t WHERE service=$1 AND namespace=$2`, ss.def.Table)
	}
	if err := seriesFilters(&b, &args, ss, q.Filters); err != nil {
		return nil, err
	}
	if !q.Since.IsZero() {
		args = append(args, q.Since)
		fmt.Fprintf(&b, " AND %s >= $%d", quoteIdent(ss.def.Time), len(args))
	}
	if !q.Until.IsZero() {
		args = append(args, q.Until)
		fmt.Fprintf(&b, " AND %s < $%d", quoteIdent(ss.def.Time), len(args))
	}
	dir := "DESC"
	if q.Ascending {
		dir = "ASC"
	}
	fmt.Fprintf(&b, " ORDER BY %s %s LIMIT %d OFFSET %d", quoteIdent(ss.def.Time), dir, q.Limit, q.Offset)

	rows, err := c.db.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SeriesPoint
	for rows.Next() {
		var p SeriesPoint
		if err := rows.Scan(&p.Namespace, &p.Data); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// seriesFilters compiles filters against the series' real columns —
// same rules as queryTable: validated field names, bound values, the
// column type driving the comparison.
func seriesFilters(b *strings.Builder, args *[]any, ss *seriesSQL, filters []Filter) error {
	for _, f := range filters {
		typ, ok := ss.colType[f.Field]
		if !ok {
			return fmt.Errorf("loom: bad filter field %q", f.Field)
		}
		if typ == "jsonb" {
			return fmt.Errorf("loom: field %q is not a scalar column and cannot be filtered", f.Field)
		}
		if f.Op == "like" {
			if typ != "text" {
				return fmt.Errorf("loom: like filter needs a string field, %q is %s", f.Field, typ)
			}
			*args = append(*args, "%"+f.Value+"%")
			fmt.Fprintf(b, " AND %s ILIKE $%d", quoteIdent(f.Field), len(*args))
			continue
		}
		op, ok := map[string]string{
			"": "=", "eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
		}[f.Op]
		if !ok {
			return fmt.Errorf("loom: bad filter op %q", f.Op)
		}
		*args = append(*args, f.Value)
		fmt.Fprintf(b, " AND %s %s $%d", quoteIdent(f.Field), op, len(*args))
	}
	return nil
}

// seriesBucketUnits are the date_trunc units buckets accept — portable
// across TimescaleDB and plain Postgres, unlike arbitrary intervals.
var seriesBucketUnits = map[string]bool{
	"hour": true, "day": true, "week": true, "month": true, "year": true,
	// "all" is one bucket for the whole range (per group): the summary
	// table shape — p50/p95 per dim over a window.
	"all": true,
}

// SeriesBucketQuery aggregates one numeric column of a series into
// time buckets, optionally grouped by dims.
type SeriesBucketQuery struct {
	Namespace     string
	AllNamespaces bool
	// Value is the numeric column to aggregate.
	Value string
	// Bucket is the date_trunc unit: hour, day, week, month, year.
	Bucket string
	// By groups buckets by these dim columns.
	By           []string
	Filters      []Filter
	Since, Until time.Time
	Limit        int // default 500, max 10000
	// Percentiles asks for percentile_cont of Value per bucket, as
	// percentages: 50, 95, 99.9. Results land in SeriesBucket.Percentiles
	// keyed "p50", "p95", "p99.9".
	Percentiles []float64
}

// SeriesBucket is one aggregated bucket. Last is the value of the most
// recent row in the bucket — the closing observation. Percentiles is
// filled only when the query asked for them.
type SeriesBucket struct {
	T           time.Time          `json:"t"`
	Group       map[string]string  `json:"group,omitempty"`
	Count       int64              `json:"count"`
	Avg         *float64           `json:"avg"`
	Min         *float64           `json:"min"`
	Max         *float64           `json:"max"`
	Last        *float64           `json:"last"`
	Percentiles map[string]float64 `json:"percentiles,omitempty"`
}

// PercentileKey names a percentile the way SeriesBucket.Percentiles is
// keyed: 50 → "p50", 99.9 → "p99.9".
func PercentileKey(p float64) string {
	return "p" + strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", p), "0"), ".")
}

// QuerySeriesBuckets is the bucketed read: count/avg/min/max/last of one
// numeric column per time bucket, oldest bucket first.
func (c *Client) QuerySeriesBuckets(ctx context.Context, series string, q SeriesBucketQuery) ([]SeriesBucket, error) {
	ss := c.series[series]
	if ss == nil {
		return nil, fmt.Errorf("loom: unknown series %s", series)
	}
	if q.Namespace == "" && !q.AllNamespaces {
		return nil, fmt.Errorf("loom: query needs a namespace")
	}
	if !seriesBucketUnits[q.Bucket] {
		return nil, fmt.Errorf("loom: bad bucket %q (hour, day, week, month, year)", q.Bucket)
	}
	switch ss.colType[q.Value] {
	case "bigint", "double precision":
	default:
		return nil, fmt.Errorf("loom: value %q is not a numeric column of %s", q.Value, ss.def.Name)
	}
	dims := map[string]bool{}
	for _, d := range ss.def.Dims {
		dims[d] = true
	}
	for _, by := range q.By {
		if !dims[by] {
			return nil, fmt.Errorf("loom: by %q is not a dim of %s (dims: %s)", by, ss.def.Name, strings.Join(ss.def.Dims, ", "))
		}
	}
	if q.Limit <= 0 {
		q.Limit = 500
	}
	if q.Limit > 10000 {
		q.Limit = 10000
	}
	fractions := make([]float64, 0, len(q.Percentiles))
	for _, p := range q.Percentiles {
		if p <= 0 || p >= 100 {
			return nil, fmt.Errorf("loom: percentile %v out of range (0, 100)", p)
		}
		fractions = append(fractions, p/100)
	}

	val, tcol := quoteIdent(q.Value), quoteIdent(ss.def.Time)
	var b strings.Builder
	if q.Bucket == "all" {
		fmt.Fprintf(&b, "SELECT min(%s) AS bt", tcol)
	} else {
		fmt.Fprintf(&b, "SELECT date_trunc('%s', %s) AS bt", q.Bucket, tcol)
	}
	for _, by := range q.By {
		fmt.Fprintf(&b, ", %s::text", quoteIdent(by))
	}
	fmt.Fprintf(&b, ", count(*), avg(%s)::float8, min(%s)::float8, max(%s)::float8, (array_agg(%s ORDER BY %s DESC))[1]::float8",
		val, val, val, val, tcol)
	var args []any
	if q.AllNamespaces {
		args = []any{c.reg.Service}
	} else {
		args = []any{c.reg.Service, q.Namespace}
	}
	if len(fractions) > 0 {
		args = append(args, fractions)
		fmt.Fprintf(&b, ", (percentile_cont($%d::float8[]) WITHIN GROUP (ORDER BY %s))::float8[]", len(args), val)
	}
	if q.AllNamespaces {
		fmt.Fprintf(&b, " FROM %s WHERE service=$1", ss.def.Table)
	} else {
		fmt.Fprintf(&b, " FROM %s WHERE service=$1 AND namespace=$2", ss.def.Table)
	}
	if err := seriesFilters(&b, &args, ss, q.Filters); err != nil {
		return nil, err
	}
	if !q.Since.IsZero() {
		args = append(args, q.Since)
		fmt.Fprintf(&b, " AND %s >= $%d", tcol, len(args))
	}
	if !q.Until.IsZero() {
		args = append(args, q.Until)
		fmt.Fprintf(&b, " AND %s < $%d", tcol, len(args))
	}
	groupCols := make([]string, 0, len(q.By)+1)
	if q.Bucket != "all" {
		groupCols = append(groupCols, "bt")
	}
	for _, by := range q.By {
		groupCols = append(groupCols, quoteIdent(by))
	}
	if len(groupCols) > 0 {
		b.WriteString(" GROUP BY " + strings.Join(groupCols, ", "))
	}
	orderCols := append([]string{"bt"}, groupCols...)
	if q.Bucket != "all" {
		orderCols = groupCols
	}
	b.WriteString(" ORDER BY " + strings.Join(orderCols, ", "))
	fmt.Fprintf(&b, " LIMIT %d", q.Limit)

	rows, err := c.db.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SeriesBucket
	for rows.Next() {
		bucket := SeriesBucket{}
		byVals := make([]string, len(q.By))
		dest := []any{&bucket.T}
		for i := range byVals {
			dest = append(dest, &byVals[i])
		}
		dest = append(dest, &bucket.Count, &bucket.Avg, &bucket.Min, &bucket.Max, &bucket.Last)
		var pcts []*float64
		if len(fractions) > 0 {
			dest = append(dest, &pcts)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		if len(fractions) > 0 {
			bucket.Percentiles = map[string]float64{}
			for i, p := range q.Percentiles {
				if i < len(pcts) && pcts[i] != nil {
					bucket.Percentiles[PercentileKey(p)] = *pcts[i]
				}
			}
		}
		if len(q.By) > 0 {
			bucket.Group = map[string]string{}
			for i, by := range q.By {
				bucket.Group[by] = byVals[i]
			}
		}
		out = append(out, bucket)
	}
	return out, rows.Err()
}
