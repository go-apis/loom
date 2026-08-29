package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
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
	var drift []string
	for _, ss := range sortedSeries(c.series) {
		def := ss.def
		if _, err := c.db.Exec(ctx, def.DDL); err != nil {
			return fmt.Errorf("loom: create %s: %w", def.Table, err)
		}
		if timescale {
			if _, err := c.db.Exec(ctx,
				`SELECT create_hypertable($1::regclass, $2::name, if_not_exists => TRUE, migrate_data => TRUE)`,
				def.Table, def.Time); err != nil {
				return fmt.Errorf("loom: hypertable %s: %w", def.Table, err)
			}
		} else {
			if _, err := c.db.Exec(ctx, fmt.Sprintf(
				`CREATE INDEX IF NOT EXISTS %s_brin ON %s USING brin (%s)`,
				def.Table, def.Table, quoteIdent(def.Time))); err != nil {
				return fmt.Errorf("loom: index %s: %w", def.Table, err)
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
		if err != nil {
			return inserted, fmt.Errorf("loom: append %s: %w", ss.def.Name, err)
		}
		inserted += tag.RowsAffected()
	}
	return inserted, nil
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
}

// SeriesBucket is one aggregated bucket. Last is the value of the most
// recent row in the bucket — the closing observation.
type SeriesBucket struct {
	T     time.Time         `json:"t"`
	Group map[string]string `json:"group,omitempty"`
	Count int64             `json:"count"`
	Avg   *float64          `json:"avg"`
	Min   *float64          `json:"min"`
	Max   *float64          `json:"max"`
	Last  *float64          `json:"last"`
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

	val, tcol := quoteIdent(q.Value), quoteIdent(ss.def.Time)
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT date_trunc('%s', %s) AS bt", q.Bucket, tcol)
	for _, by := range q.By {
		fmt.Fprintf(&b, ", %s::text", quoteIdent(by))
	}
	fmt.Fprintf(&b, ", count(*), avg(%s)::float8, min(%s)::float8, max(%s)::float8, (array_agg(%s ORDER BY %s DESC))[1]::float8",
		val, val, val, val, tcol)
	var args []any
	if q.AllNamespaces {
		args = []any{c.reg.Service}
		fmt.Fprintf(&b, " FROM %s WHERE service=$1", ss.def.Table)
	} else {
		args = []any{c.reg.Service, q.Namespace}
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
	b.WriteString(" GROUP BY bt")
	for _, by := range q.By {
		fmt.Fprintf(&b, ", %s", quoteIdent(by))
	}
	b.WriteString(" ORDER BY bt")
	for _, by := range q.By {
		fmt.Fprintf(&b, ", %s", quoteIdent(by))
	}
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
		if err := rows.Scan(dest...); err != nil {
			return nil, err
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
