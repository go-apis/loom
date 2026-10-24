package e2e_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// TestListReadSelectsAskedFields proves Query.Fields end to end on the
// @table path: a list read with Fields returns Data holding only the asked
// keys (filters and order on unasked columns still apply), and Fields nil
// returns the whole document.
func TestListReadSelectsAskedFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	customer := uuid.New()
	placeOrder(t, ctx, cli, uuid.New(), customer, 500)
	placeOrder(t, ctx, cli, uuid.New(), customer, 9000)
	waitFor(t, ctx, "summaries projected", func() bool {
		rows, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{Namespace: "default"})
		return err == nil && len(rows) == 2
	})

	keys := func(r loom.Row) map[string]json.RawMessage {
		t.Helper()
		var m map[string]json.RawMessage
		if err := json.Unmarshal(r.Data, &m); err != nil {
			t.Fatalf("Data is not an object: %s", r.Data)
		}
		return m
	}

	rows, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{
		Namespace: "default",
		Fields:    []string{"id", "status", "currency"},
		Filters:   []loom.Filter{{Field: "total_cents", Op: "gte", Value: "1000"}},
		OrderBy:   "-total_cents",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 filtered row, got %d", len(rows))
	}
	m := keys(rows[0])
	if len(m) != 2 || string(m["status"]) != `"placed"` || string(m["currency"]) != `"USD"` {
		t.Fatalf("asked-fields Data wrong: %s", rows[0].Data)
	}
	if rows[0].ID == "" || rows[0].Namespace != "default" || rows[0].UpdatedAt.IsZero() {
		t.Fatalf("meta columns missing: %+v", rows[0])
	}

	rows, err = cli.QueryEntities(ctx, "OrderSummary", loom.Query{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		m := keys(r)
		for _, k := range []string{"status", "customer_id", "total_cents", "currency", "reason", "items"} {
			if _, ok := m[k]; !ok {
				t.Fatalf("whole-row Data lacks %s: %s", k, r.Data)
			}
		}
	}

	if _, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{Namespace: "default", Fields: []string{"nope"}}); err == nil {
		t.Fatal("unknown field accepted")
	}
}
