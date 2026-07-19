package loom

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// Typed entity tables (@table). The generated TableDef owns the shape —
// DDL, column list, typed value extractor — and everything here is built
// once at boot: the write path never assembles SQL per event and never
// reflects.

type tableSQL struct {
	def             *TableDef
	upsert          string
	selectDoc       string // one row, doc-shaped (fold/Entity reads)
	selectForUpdate string
}

func buildTables(reg *Registry) map[string]*tableSQL {
	out := map[string]*tableSQL{}
	for _, def := range reg.Tables {
		out[def.Entity] = &tableSQL{
			def:             def,
			upsert:          tableUpsert(def),
			selectDoc:       tableSelectDoc(def, false),
			selectForUpdate: tableSelectDoc(def, true),
		}
	}
	return out
}

// tableSelectDoc reads one row back into the shared doc shape: to_jsonb of
// the row minus the meta columns is exactly the entity state document, so
// fold loading and typed reads reuse the same unmarshal path as the doc
// store.
func tableSelectDoc(def *TableDef, forUpdate bool) string {
	q := fmt.Sprintf(
		`SELECT to_jsonb(t) - 'service' - 'namespace' - 'id' - 'updated_at' FROM %s t WHERE service=$1 AND namespace=$2 AND id=$3`,
		def.Name)
	if forUpdate {
		q += " FOR UPDATE"
	}
	return q
}

func tableUpsert(def *TableDef) string {
	var cols, params, sets []string
	for i, col := range def.Columns {
		cols = append(cols, quoteIdent(col.Name))
		params = append(params, fmt.Sprintf("$%d", i+4))
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(col.Name), quoteIdent(col.Name)))
	}
	sets = append(sets, "updated_at = now()")
	insertCols := append([]string{"service", "namespace", "id"}, cols...)
	insertParams := append([]string{"$1", "$2", "$3"}, params...)
	return fmt.Sprintf(
		`INSERT INTO %s (%s, updated_at) VALUES (%s, now()) ON CONFLICT (service, namespace, id) DO UPDATE SET %s`,
		def.Name, strings.Join(insertCols, ", "), strings.Join(insertParams, ", "), strings.Join(sets, ", "))
}

func quoteIdent(name string) string {
	return `"` + name + `"`
}

// migrateTables applies each @table entity's DDL, then diffs the live
// column shape against the declaration. The diff is additive-only: missing
// columns are added, extra live columns are left alone, and a type
// mismatch is an error — never an ALTER TYPE or a drop. This is the
// deliberate opposite of AutoMigrate: projections are rebuildable, so the
// remediation for incompatible drift is drop table, Migrate, Rebuild.
func (c *Client) migrateTables(ctx context.Context) error {
	var drift []string
	for _, ts := range sortedTables(c.tables) {
		def := ts.def
		pre, err := c.liveColumns(ctx, def.Name)
		if err != nil {
			return err
		}
		existed := len(pre) > 0
		if _, err := c.db.Exec(ctx, def.DDL); err != nil {
			return fmt.Errorf("loom: create %s: %w", def.Name, err)
		}
		// a @table aggregate's mirror created after the aggregate has
		// history starts empty — replay every stream into it once
		if !existed {
			if err := c.backfillAggregateTable(ctx, ts); err != nil {
				return err
			}
		}
		live, err := c.liveColumns(ctx, def.Name)
		if err != nil {
			return err
		}
		for _, col := range def.Columns {
			liveType, ok := live[col.Name]
			if !ok {
				if _, err := c.db.Exec(ctx, fmt.Sprintf(
					`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s`,
					def.Name, quoteIdent(col.Name), col.Type)); err != nil {
					return fmt.Errorf("loom: add %s.%s: %w", def.Name, col.Name, err)
				}
				continue
			}
			if liveType != pgTypeName(col.Type) {
				drift = append(drift, fmt.Sprintf(
					"%s.%s is %s, schema wants %s", def.Name, col.Name, liveType, col.Type))
			}
		}
	}
	if len(drift) > 0 {
		return fmt.Errorf(
			"loom: incompatible table drift (the read model is rebuildable — drop the table, Migrate, then Rebuild the projection):\n  - %s",
			strings.Join(drift, "\n  - "))
	}
	return nil
}

// backfillAggregateTable fills a freshly created @table aggregate mirror
// from the event log: every stream of the aggregate type is loaded
// (snapshot + events) and upserted. Entity tables are skipped — their
// remediation is projection Rebuild.
func (c *Client) backfillAggregateTable(ctx context.Context, ts *tableSQL) error {
	var agg *AggregateDef
	for _, a := range c.reg.Aggregates {
		if a.Name == ts.def.Entity {
			agg = a
		}
	}
	if agg == nil {
		return nil
	}
	rows, err := c.db.Query(ctx, `
		SELECT DISTINCT namespace, aggregate_id FROM loom_events
		WHERE service=$1 AND aggregate_type=$2`, c.reg.Service, agg.Name)
	if err != nil {
		return err
	}
	type stream struct {
		ns string
		id uuid.UUID
	}
	var streams []stream
	for rows.Next() {
		var s stream
		if err := rows.Scan(&s.ns, &s.id); err != nil {
			rows.Close()
			return err
		}
		streams = append(streams, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, s := range streams {
		state, version, err := c.loadState(ctx, c.db, agg, s.ns, s.id)
		if err != nil {
			return fmt.Errorf("loom: backfill %s %s/%s: %w", agg.Name, s.ns, s.id, err)
		}
		if version == 0 {
			continue
		}
		args := append([]any{c.reg.Service, s.ns, s.id}, ts.def.Values(state)...)
		if _, err := c.db.Exec(ctx, ts.upsert, args...); err != nil {
			return fmt.Errorf("loom: backfill %s %s/%s: %w", agg.Name, s.ns, s.id, err)
		}
	}
	return nil
}

func (c *Client) liveColumns(ctx context.Context, table string) (map[string]string, error) {
	rows, err := c.db.Query(ctx, `
		SELECT column_name, data_type FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out[name] = typ
	}
	return out, rows.Err()
}

// pgTypeName maps a DDL type to information_schema.columns.data_type.
func pgTypeName(ddlType string) string {
	switch ddlType {
	case "timestamptz":
		return "timestamp with time zone"
	default:
		return ddlType
	}
}

func sortedTables(tables map[string]*tableSQL) []*tableSQL {
	out := make([]*tableSQL, 0, len(tables))
	for _, ts := range tables {
		out = append(out, ts)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].def.Name < out[j].def.Name })
	return out
}

// queryTable is the typed-table twin of queryDocs: same Query in, same Row
// out, but filters and ORDER BY compile to real columns — no jsonb casts.
func (c *Client) queryTable(ctx context.Context, ts *tableSQL, q Query) ([]Row, error) {
	if q.Namespace == "" && !q.AllNamespaces {
		return nil, fmt.Errorf("loom: query needs a namespace")
	}
	if q.Limit <= 0 {
		q.Limit = 100
	}
	if q.Limit > 500 {
		q.Limit = 500
	}
	colType := map[string]string{}
	for _, col := range ts.def.Columns {
		colType[col.Name] = col.Type
	}

	var b strings.Builder
	var args []any
	if q.AllNamespaces {
		args = []any{c.reg.Service}
		fmt.Fprintf(&b,
			`SELECT id, namespace, updated_at, to_jsonb(t) - 'service' - 'namespace' - 'id' - 'updated_at' FROM %s t WHERE service=$1`,
			ts.def.Name)
	} else {
		args = []any{c.reg.Service, q.Namespace}
		fmt.Fprintf(&b,
			`SELECT id, namespace, updated_at, to_jsonb(t) - 'service' - 'namespace' - 'id' - 'updated_at' FROM %s t WHERE service=$1 AND namespace=$2`,
			ts.def.Name)
	}

	for _, f := range q.Filters {
		typ, ok := colType[f.Field]
		if !ok {
			return nil, fmt.Errorf("loom: bad filter field %q", f.Field)
		}
		if typ == "jsonb" {
			return nil, fmt.Errorf("loom: field %q is not a scalar column and cannot be filtered", f.Field)
		}
		if f.Op == "like" {
			if typ != "text" {
				return nil, fmt.Errorf("loom: like filter needs a string field, %q is %s", f.Field, typ)
			}
			args = append(args, "%"+f.Value+"%")
			fmt.Fprintf(&b, " AND %s ILIKE $%d", quoteIdent(f.Field), len(args))
			continue
		}
		op, ok := map[string]string{
			"": "=", "eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
		}[f.Op]
		if !ok {
			return nil, fmt.Errorf("loom: bad filter op %q", f.Op)
		}
		// the column type drives the comparison: the text value is bound as
		// an untyped parameter and Postgres parses it as the column's type
		args = append(args, f.Value)
		fmt.Fprintf(&b, " AND %s %s $%d", quoteIdent(f.Field), op, len(args))
	}

	orderCol, desc := "updated_at", true
	if q.OrderBy != "" {
		field := strings.TrimPrefix(q.OrderBy, "-")
		desc = strings.HasPrefix(q.OrderBy, "-")
		if field != "updated_at" {
			if _, ok := colType[field]; !ok {
				return nil, fmt.Errorf("loom: bad order field %q", field)
			}
			orderCol = quoteIdent(field)
		}
	}
	dir := "ASC"
	if desc {
		dir = "DESC"
	}
	fmt.Fprintf(&b, " ORDER BY %s %s LIMIT %d OFFSET %d", orderCol, dir, q.Limit, q.Offset)

	rows, err := c.db.Query(ctx, b.String(), args...)
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
