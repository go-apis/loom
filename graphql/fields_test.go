package graphql

import (
	"context"
	"reflect"
	"testing"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

type orderSummaryState struct {
	Status     string `json:"status"`
	TotalCents int64  `json:"total_cents"`
	Region     string `json:"region"`
}

// schemaForList builds a minimal one-field schema around docList, the
// way b.service wires a real aggregate/record/projection list — close
// enough to exercise queryFromArgs and listQueryFields through an actual
// graphql-go execution, without standing up a loom.Client.
func schemaForList(t *testing.T) (gql.Schema, *loom.Query) {
	t.Helper()
	return schemaForListWith(t, nil)
}

// schemaForListWith is schemaForList over a @table mirror when td is set.
func schemaForListWith(t *testing.T, td *loom.TableDef) (gql.Schema, *loom.Query) {
	t.Helper()
	b := &builder{types: map[string]*typeEntry{}}
	obj, err := b.objectFor("OrderSummary", orderSummaryState{})
	if err != nil {
		t.Fatal(err)
	}
	byCamel := camelFieldMap(b.types["OrderSummary"].fields)
	if td != nil {
		byCamel = tableFieldMap(b.types["OrderSummary"].fields, td)
	}
	var captured loom.Query
	query := func(ctx context.Context, q loom.Query) ([]loom.Row, error) {
		captured = q
		return nil, nil
	}
	schema, err := gql.NewSchema(gql.SchemaConfig{
		Query: gql.NewObject(gql.ObjectConfig{
			Name: "Query",
			Fields: gql.Fields{
				"orderSummarys": docList(obj, byCamel, nil, query),
			},
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema, &captured
}

func run(t *testing.T, schema gql.Schema, query string) {
	t.Helper()
	res := gql.Do(gql.Params{Schema: schema, RequestString: query, Context: context.Background()})
	if len(res.Errors) > 0 {
		t.Fatalf("query errors: %v", res.Errors)
	}
}

func TestGraphQLListSelectsOnlyAskedFields(t *testing.T) {
	t.Run("only the asked state fields, snake case, meta excluded", func(t *testing.T) {
		schema, captured := schemaForList(t)
		run(t, schema, `{ orderSummarys(namespace:"default") { id namespace updatedAt status totalCents } }`)
		if got, want := captured.Fields, []string{"status", "total_cents"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Fields = %v, want %v", got, want)
		}
	})

	t.Run("fragment selection is followed", func(t *testing.T) {
		schema, captured := schemaForList(t)
		run(t, schema, `{ orderSummarys(namespace:"default") { ...F } } fragment F on OrderSummary { status totalCents }`)
		if got, want := captured.Fields, []string{"status", "total_cents"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Fields = %v, want %v", got, want)
		}
	})

	t.Run("a filter on a field not selected still works", func(t *testing.T) {
		schema, captured := schemaForList(t)
		run(t, schema, `{ orderSummarys(namespace:"default", where:[{field:"region",op:EQ,value:"us"}]) { status } }`)
		if got, want := captured.Fields, []string{"status"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Fields = %v, want %v", got, want)
		}
		if len(captured.Filters) != 1 || captured.Filters[0].Field != "region" {
			t.Fatalf("Filters = %v, want a filter on region", captured.Filters)
		}
	})

	t.Run("a @table mirror never asks for a field it has no column for", func(t *testing.T) {
		// region stands in for a @pii field: a state field, but not a column
		td := &loom.TableDef{Columns: []loom.TableColumn{{Name: "status"}, {Name: "total_cents"}}}
		schema, captured := schemaForListWith(t, td)
		run(t, schema, `{ orderSummarys(namespace:"default") { id status region } }`)
		if got, want := captured.Fields, []string{"status"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Fields = %v, want %v", got, want)
		}
	})
}
