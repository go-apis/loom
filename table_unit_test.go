package loom

import (
	"strings"
	"testing"
)

func TestListSQLOmitsUnaskedColumns(t *testing.T) {
	def := &TableDef{
		Name: "loom_t_svc_thing",
		Columns: []TableColumn{
			{Name: "status", Type: "text"},
			{Name: "total_cents", Type: "bigint"},
			{Name: "heavy", Type: "text"},
		},
	}
	base := Query{
		Namespace: "default",
		Filters:   []Filter{{Field: "total_cents", Op: "gte", Value: "10"}},
		OrderBy:   "-total_cents",
		Limit:     20,
	}

	// Fields nil: the whole-row read, byte for byte as before
	whole, args, err := tableListSQL(def, "svc", base)
	if err != nil {
		t.Fatal(err)
	}
	wantWhole := `SELECT id, namespace, updated_at, to_jsonb(t) - 'service' - 'namespace' - 'id' - 'updated_at' FROM loom_t_svc_thing t WHERE service=$1 AND namespace=$2 AND "total_cents" >= $3 ORDER BY "total_cents" DESC NULLS LAST LIMIT 20 OFFSET 0`
	if whole != wantWhole {
		t.Fatalf("whole-row SQL changed:\n got %s\nwant %s", whole, wantWhole)
	}
	if len(args) != 3 || args[0] != "svc" || args[1] != "default" || args[2] != "10" {
		t.Fatalf("args: %v", args)
	}
	empty := base
	empty.Fields = []string{}
	if s, _, _ := tableListSQL(def, "svc", empty); s != wantWhole {
		t.Fatalf("empty Fields must read the whole row: %s", s)
	}

	// Fields set: only the asked columns, filters and order on others still work
	asked := base
	asked.Fields = []string{"status"}
	sql, _, err := tableListSQL(def, "svc", asked)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "heavy") || strings.Contains(sql, "to_jsonb(t)") {
		t.Fatalf("unasked column read: %s", sql)
	}
	for _, want := range []string{`jsonb_build_object('status', "status")`, `"total_cents" >= $3`, `ORDER BY "total_cents"`} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %s in %s", want, sql)
		}
	}

	// naming heavy puts it in; meta fields are skipped
	asked.Fields = []string{"id", "status", "heavy"}
	sql, _, err = tableListSQL(def, "svc", asked)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `jsonb_build_object('status', "status", 'heavy', "heavy")`) {
		t.Fatalf("named heavy not selected: %s", sql)
	}

	// an unknown field is an error, like an unknown filter field
	asked.Fields = []string{"status", `x"; DROP TABLE t; --`}
	if _, _, err := tableListSQL(def, "svc", asked); err == nil {
		t.Fatal("unknown field accepted")
	}

	// the jsonb-document path: same opt-in
	docs, _, err := docsListSQL("loom_entities", "entity_type", "Thing", "svc", Query{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	wantDocs := `SELECT id, namespace, updated_at, data FROM loom_entities WHERE service=$1 AND namespace=$2 AND entity_type=$3 ORDER BY updated_at DESC NULLS LAST LIMIT 100 OFFSET 0`
	if docs != wantDocs {
		t.Fatalf("whole-doc SQL changed:\n got %s\nwant %s", docs, wantDocs)
	}
	docs, _, err = docsListSQL("loom_entities", "entity_type", "Thing", "svc", Query{Namespace: "default", Fields: []string{"status"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(docs, `SELECT id, namespace, updated_at, jsonb_build_object('status', data->'status') FROM`) || strings.Contains(docs, "heavy") {
		t.Fatalf("doc fields SQL: %s", docs)
	}
	if _, _, err := docsListSQL("loom_entities", "entity_type", "Thing", "svc", Query{Namespace: "default", Fields: []string{"Bad-Name"}}); err == nil {
		t.Fatal("bad doc field accepted")
	}

	// more than 50 fields chunks under Postgres's 100-argument limit
	pairs := make([]string, 51)
	for i := range pairs {
		pairs[i] = "'a', 1"
	}
	if got := buildObjectSQL(pairs); strings.Count(got, "jsonb_build_object(") != 2 || !strings.Contains(got, ") || jsonb_build_object(") {
		t.Fatalf("chunking: %s", got)
	}
}
