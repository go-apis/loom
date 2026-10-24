package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The entity query layer: filtered, ordered, paginated reads over read
// models (and records). Filters compile to parameterized jsonb SQL; field
// names are validated, values are always bound parameters.

type Filter struct {
	Field string
	Op    string // eq | ne | gt | gte | lt | lte | like
	Value string
}

type Query struct {
	Namespace string
	// AllNamespaces searches across every namespace (Namespace empty) —
	// the god-mode read; rows carry their namespace either way.
	AllNamespaces bool
	Filters       []Filter
	// OrderBy is a state field, or "updated_at"; prefix "-" for descending.
	OrderBy string
	Limit   int
	Offset  int
	// Fields, when set, narrows each Row.Data to the named state fields —
	// the list read selects only those, so unasked columns are never read.
	// Nil or empty reads the whole row. The meta fields (id, namespace,
	// updated_at) ride every Row regardless and are ignored here. Filters
	// and OrderBy may name fields that Fields leaves out.
	Fields []string
}

// Row is one query hit: the entity/record id plus its state document.
type Row struct {
	ID        string          `json:"id"`
	Namespace string          `json:"namespace"`
	UpdatedAt time.Time       `json:"updated_at"`
	Data      json.RawMessage `json:"data"`
}

var fieldRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// QueryEntities searches a read model: a projection's entity, or a @table
// aggregate's state mirror (registered under the aggregate's name).
func (c *Client) QueryEntities(ctx context.Context, entity string, q Query) ([]Row, error) {
	if table := c.tables[entity]; table != nil {
		return c.queryTable(ctx, table, q)
	}
	found := false
	for _, p := range c.reg.Projections {
		if p.Entity == entity {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("loom: unknown entity %s", entity)
	}
	return c.queryDocs(ctx, "loom_entities", "entity_type", entity, q)
}

// QueryRecords searches state-of-record objects.
func (c *Client) QueryRecords(ctx context.Context, record string, q Query) ([]Row, error) {
	found := false
	for _, r := range c.reg.Records {
		if r.Name == record {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("loom: unknown record %s", record)
	}
	return c.queryDocs(ctx, "loom_records", "record_type", record, q)
}

func (c *Client) queryDocs(ctx context.Context, table, typeCol, typeName string, q Query) ([]Row, error) {
	query, args, err := docsListSQL(table, typeCol, typeName, c.reg.Service, q)
	if err != nil {
		return nil, err
	}
	return c.scanRows(ctx, query, args)
}

// docsListSQL builds the jsonb-document list read. Pure, so the SQL shape
// is unit-testable without Postgres.
func docsListSQL(table, typeCol, typeName, service string, q Query) (string, []any, error) {
	if q.Namespace == "" && !q.AllNamespaces {
		return "", nil, fmt.Errorf("loom: query needs a namespace")
	}
	q.Limit = clampLimit(q.Limit)

	doc := "data"
	if len(q.Fields) > 0 {
		var pairs []string
		for _, f := range q.Fields {
			if metaField(f) {
				continue
			}
			if !fieldRe.MatchString(f) {
				return "", nil, fmt.Errorf("loom: bad field %q", f)
			}
			pairs = append(pairs, fmt.Sprintf("'%s', data->'%s'", f, f))
		}
		doc = buildObjectSQL(pairs)
	}

	var b strings.Builder
	var args []any
	if q.AllNamespaces {
		args = []any{service, typeName}
		fmt.Fprintf(&b, `SELECT id, namespace, updated_at, %s FROM %s WHERE service=$1 AND %s=$2`, doc, table, typeCol)
	} else {
		args = []any{service, q.Namespace, typeName}
		fmt.Fprintf(&b, `SELECT id, namespace, updated_at, %s FROM %s WHERE service=$1 AND namespace=$2 AND %s=$3`, doc, table, typeCol)
	}

	for _, f := range q.Filters {
		frag, arg, err := filterSQL(f)
		if err != nil {
			return "", nil, err
		}
		args = append(args, arg)
		fmt.Fprintf(&b, " AND %s$%d)", frag, len(args))
	}

	orderCol, desc := "updated_at", true
	if q.OrderBy != "" {
		field := strings.TrimPrefix(q.OrderBy, "-")
		desc = strings.HasPrefix(q.OrderBy, "-")
		if field == "updated_at" {
			orderCol = "updated_at"
		} else {
			if !fieldRe.MatchString(field) {
				return "", nil, fmt.Errorf("loom: bad order field %q", field)
			}
			orderCol = fmt.Sprintf("data->>'%s'", field)
		}
	}
	dir := "ASC"
	if desc {
		// Postgres defaults DESC to NULLS FIRST, so every row missing the
		// order field would pin to the top of a newest-first list (e.g. a
		// backfilled invoice with no requested_at leading the admin list
		// forever). Rows without the field sort last in both directions.
		dir = "DESC NULLS LAST"
	}
	fmt.Fprintf(&b, " ORDER BY %s %s LIMIT %d OFFSET %d", orderCol, dir, q.Limit, q.Offset)
	return b.String(), args, nil
}

func clampLimit(n int) int {
	if n <= 0 {
		return 100
	}
	if n > 500 {
		return 500
	}
	return n
}

// metaField reports a Row meta column, which every list read returns
// outside Data; naming one in Query.Fields is a no-op.
func metaField(f string) bool {
	return f == "id" || f == "namespace" || f == "updated_at"
}

// buildObjectSQL renders jsonb_build_object over "'key', value" pairs,
// chunked and joined with || because a Postgres function takes at most 100
// arguments (50 pairs).
func buildObjectSQL(pairs []string) string {
	if len(pairs) == 0 {
		return "'{}'::jsonb"
	}
	var parts []string
	for len(pairs) > 0 {
		n := min(len(pairs), 50)
		parts = append(parts, "jsonb_build_object("+strings.Join(pairs[:n], ", ")+")")
		pairs = pairs[n:]
	}
	return strings.Join(parts, " || ")
}

// scanRows runs a list read and scans its (id, namespace, updated_at, doc)
// rows.
func (c *Client) scanRows(ctx context.Context, query string, args []any) ([]Row, error) {
	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Namespace, &r.UpdatedAt, &r.Data); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// filterSQL compiles one filter to a fragment ending just before the bound
// parameter. Numeric and boolean literals compare typed; everything else
// compares as text.
func filterSQL(f Filter) (frag string, arg any, err error) {
	if !fieldRe.MatchString(f.Field) {
		return "", nil, fmt.Errorf("loom: bad filter field %q", f.Field)
	}
	if f.Op == "like" {
		return fmt.Sprintf("((data->>'%s') ILIKE ", f.Field), "%" + f.Value + "%", nil
	}
	op, ok := map[string]string{
		"": "=", "eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
	}[f.Op]
	if !ok {
		return "", nil, fmt.Errorf("loom: bad filter op %q", f.Op)
	}
	if n, nerr := strconv.ParseFloat(f.Value, 64); nerr == nil {
		return fmt.Sprintf("((data->>'%s')::numeric %s ", f.Field, op), n, nil
	}
	if f.Value == "true" || f.Value == "false" {
		return fmt.Sprintf("((data->>'%s')::boolean %s ", f.Field, op), f.Value == "true", nil
	}
	return fmt.Sprintf("((data->>'%s') %s ", f.Field, op), f.Value, nil
}

// LogQuery filters the service's slice of the event log.
type LogQuery struct {
	Namespace     string
	Type          string
	AggregateType string
	AggregateID   string
	CorrelationID string
	AfterSeq      int64
	Since         time.Time
	Until         time.Time
	Limit         int
	Ascending     bool
}

// LogEntry is one event row with its payload left raw (foreign or legacy
// types stay browsable even without a local registration).
type LogEntry struct {
	GlobalSeq     int64           `json:"global_seq"`
	Namespace     string          `json:"namespace"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Version       int             `json:"version"`
	Type          string          `json:"type"`
	SchemaVersion int             `json:"schema_version"`
	At            time.Time       `json:"at"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	CausationID   string          `json:"causation_id,omitempty"`
	Actor         string          `json:"actor,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// QueryLog browses the event log. Time-range scans ride the BRIN index —
// the log is the timeseries.
func (c *Client) QueryLog(ctx context.Context, q LogQuery) ([]LogEntry, error) {
	if q.Limit <= 0 {
		q.Limit = 100
	}
	if q.Limit > 1000 {
		q.Limit = 1000
	}
	var b strings.Builder
	args := []any{c.reg.Service}
	b.WriteString(`SELECT global_seq, namespace, aggregate_type, aggregate_id, version, type, schema_version, at, correlation_id, causation_id, actor, data
		FROM loom_events WHERE service=$1`)
	and := func(frag string, v any) {
		args = append(args, v)
		fmt.Fprintf(&b, " AND %s$%d", frag, len(args))
	}
	if q.Namespace != "" {
		and("namespace=", q.Namespace)
	}
	if q.Type != "" {
		and("type=", q.Type)
	}
	if q.AggregateType != "" {
		and("aggregate_type=", q.AggregateType)
	}
	if q.AggregateID != "" {
		and("aggregate_id=", q.AggregateID)
	}
	if q.CorrelationID != "" {
		and("correlation_id=", q.CorrelationID)
	}
	if q.AfterSeq > 0 {
		and("global_seq>", q.AfterSeq)
	}
	if !q.Since.IsZero() {
		and("at>=", q.Since)
	}
	if !q.Until.IsZero() {
		and("at<", q.Until)
	}
	dir := "DESC"
	if q.Ascending {
		dir = "ASC"
	}
	fmt.Fprintf(&b, " ORDER BY global_seq %s LIMIT %d", dir, q.Limit)

	rows, err := c.db.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LogEntry
	for rows.Next() {
		var e LogEntry
		if err := rows.Scan(&e.GlobalSeq, &e.Namespace, &e.AggregateType, &e.AggregateID, &e.Version, &e.Type, &e.SchemaVersion, &e.At, &e.CorrelationID, &e.CausationID, &e.Actor, &e.Data); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// LogStats counts events by type since a time — the rate/volume view.
func (c *Client) LogStats(ctx context.Context, since time.Time) (map[string]int64, error) {
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	rows, err := c.db.Query(ctx, `
		SELECT type, count(*) FROM loom_events
		WHERE service=$1 AND at >= $2
		GROUP BY type`,
		c.reg.Service, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var t string
		var n int64
		if err := rows.Scan(&t, &n); err != nil {
			return nil, err
		}
		out[t] = n
	}
	return out, rows.Err()
}
