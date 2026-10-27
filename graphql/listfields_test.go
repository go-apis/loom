package graphql

import (
	"context"
	"reflect"
	"testing"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// TestListFieldsFromSelection runs listFields through a real graphql-go
// execution — the same path selectedFields relies on — so p.Info.Fragments
// and p.Info.FieldASTs are populated exactly as the live gateway sees them,
// not hand-built.
func TestListFieldsFromSelection(t *testing.T) {
	var captured []string

	customer := gql.NewObject(gql.ObjectConfig{
		Name: "Customer",
		Fields: gql.Fields{
			"name":  &gql.Field{Type: gql.String},
			"email": &gql.Field{Type: gql.String},
		},
	})

	item := gql.NewObject(gql.ObjectConfig{
		Name: "Item",
		Fields: gql.Fields{
			"id":         &gql.Field{Type: gql.String},
			"namespace":  &gql.Field{Type: gql.String},
			"updatedAt":  &gql.Field{Type: gql.String},
			"dueDate":    &gql.Field{Type: gql.String},
			"totalCents": &gql.Field{Type: gql.Int},
			"status":     &gql.Field{Type: gql.String},
			"region":     &gql.Field{Type: gql.String},
			"customer":   &gql.Field{Type: customer},
		},
	})

	query := gql.NewObject(gql.ObjectConfig{
		Name: "Query",
		Fields: gql.Fields{
			"items": &gql.Field{
				Type: gql.NewList(item),
				Resolve: func(p gql.ResolveParams) (any, error) {
					captured = listFields(p)
					return nil, nil
				},
			},
		},
	})

	schema, err := gql.NewSchema(gql.SchemaConfig{Query: query})
	if err != nil {
		t.Fatal(err)
	}

	run := func(q string) []string {
		captured = nil
		res := gql.Do(gql.Params{Schema: schema, RequestString: q, Context: context.Background()})
		if len(res.Errors) > 0 {
			t.Fatalf("query %q: %v", q, res.Errors)
		}
		return captured
	}

	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{
			"plain fields",
			`{ items { status region } }`,
			[]string{"status", "region"},
		},
		{
			"camel to snake",
			`{ items { dueDate totalCents } }`,
			[]string{"due_date", "total_cents"},
		},
		{
			"meta excluded",
			`{ items { id namespace updatedAt __typename dueDate } }`,
			[]string{"due_date"},
		},
		{
			"inline fragment followed",
			`{ items { ... on Item { status dueDate } } }`,
			[]string{"status", "due_date"},
		},
		{
			"named fragment spread followed",
			`{ items { ...f } } fragment f on Item { status region }`,
			[]string{"status", "region"},
		},
		{
			"nested object selection contributes only its top-level name",
			`{ items { customer { name email } dueDate } }`,
			[]string{"customer", "due_date"},
		},
		{
			"only-meta selection returns nil",
			`{ items { id namespace updatedAt __typename } }`,
			nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := run(tc.query)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("listFields(%s) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

// TestGraphQLListSelectsOnlyAskedFields runs a real docList resolver
// through graphql-go and checks the loom.Query it hands its query func:
// Fields must match listFields' own view of the selection, and a
// meta-only selection must leave Fields nil (whole-row default).
func TestGraphQLListSelectsOnlyAskedFields(t *testing.T) {
	order := gql.NewObject(gql.ObjectConfig{
		Name: "Order",
		Fields: gql.Fields{
			"id":        &gql.Field{Type: gql.String},
			"namespace": &gql.Field{Type: gql.String},
			"updatedAt": &gql.Field{Type: gql.String},
			"status":    &gql.Field{Type: gql.String},
			"dueDate":   &gql.Field{Type: gql.String},
			"total":     &gql.Field{Type: gql.Int},
		},
	})

	var captured loom.Query
	queryFn := func(ctx context.Context, q loom.Query) ([]loom.Row, error) {
		captured = q
		return nil, nil
	}

	query := gql.NewObject(gql.ObjectConfig{
		Name:   "Query",
		Fields: gql.Fields{"orders": docList(order, nil, queryFn, func(f []string) []string { return f })},
	})

	schema, err := gql.NewSchema(gql.SchemaConfig{Query: query})
	if err != nil {
		t.Fatal(err)
	}

	run := func(q string) loom.Query {
		captured = loom.Query{}
		res := gql.Do(gql.Params{Schema: schema, RequestString: q, Context: context.Background()})
		if len(res.Errors) > 0 {
			t.Fatalf("query %q: %v", q, res.Errors)
		}
		return captured
	}

	got := run(`{ orders(namespace: "ns") { id status dueDate ...F } } fragment F on Order { total }`)
	if want := []string{"status", "due_date", "total"}; !reflect.DeepEqual(got.Fields, want) {
		t.Errorf("Fields = %v, want %v", got.Fields, want)
	}

	got = run(`{ orders(namespace: "ns") { id namespace updatedAt } }`)
	if got.Fields != nil {
		t.Errorf("meta-only selection: Fields = %v, want nil", got.Fields)
	}
}

// TestNarrowerKeepsOnlyServableFields: a list read is narrowed to the
// asked state fields that are columns of its @table — a @pii field with
// no column is dropped (it resolves null either way) — and any asked join
// widens the read back to the whole row, since a join's resolver reads
// the source row as it likes (#83/#87 asked for both and the read failed
// with `bad field`).
func TestNarrowerKeepsOnlyServableFields(t *testing.T) {
	b := &builder{types: map[string]*typeEntry{
		"Payee": {fields: []string{"bank_token", "name", "tin", "tin_last4"}},
	}}
	table := &loom.TableDef{Entity: "Payee", Columns: []loom.TableColumn{{Name: "name"}, {Name: "tin_last4"}}}
	narrow := b.narrower("Payee", table)
	if got, want := narrow([]string{"name", "tin_last4", "tin"}), []string{"name", "tin_last4"}; !reflect.DeepEqual(got, want) {
		t.Errorf("pii field: got %v, want %v", got, want)
	}
	if got := narrow([]string{"name", "invoice"}); got != nil {
		t.Errorf("join asked: got %v, want nil (whole row)", got)
	}
	if got := narrow([]string{"tin"}); len(got) != 0 {
		t.Errorf("only a column-less field: got %v, want none (whole row)", got)
	}
	// A record or document read has no table: own fields pass as asked.
	if got, want := b.narrower("Payee", nil)([]string{"tin", "name"}), []string{"tin", "name"}; !reflect.DeepEqual(got, want) {
		t.Errorf("no table: got %v, want %v", got, want)
	}
}
