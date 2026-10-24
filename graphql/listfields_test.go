package graphql

import (
	"context"
	"reflect"
	"testing"

	gql "github.com/graphql-go/graphql"
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
