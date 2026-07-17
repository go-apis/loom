package e2e_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestTypedEntityTable proves the @table storage path end to end:
// OrderSummary lives in loom_t_orders_order_summary with real columns, the
// projection upserts typed values (items on a jsonb column), queries filter
// on real columns, Rebuild refolds into the table, and Migrate's diff is
// additive-only — re-adding dropped columns, erroring on type drift.
func TestTypedEntityTable(t *testing.T) {
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
	cheap, dear := uuid.New(), uuid.New()
	placeOrder(t, ctx, cli, cheap, customer, 500)
	placeOrder(t, ctx, cli, dear, customer, 9000)

	waitFor(t, ctx, "summaries projected", func() bool {
		rows, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{Namespace: "default"})
		return err == nil && len(rows) == 2
	})

	// the row is real columns, readable as plain SQL
	var status, currency string
	var cents int64
	var itemsRaw []byte
	err = pool.QueryRow(ctx, `
		SELECT "status", "currency", "total_cents", "items"
		FROM loom_t_orders_order_summary
		WHERE service='orders' AND namespace='default' AND id=$1`, dear).
		Scan(&status, &currency, &cents, &itemsRaw)
	if err != nil {
		t.Fatal(err)
	}
	if status != "placed" || currency != "USD" || cents != 9000 {
		t.Fatalf("typed columns wrong: %s %s %d", status, currency, cents)
	}
	var items []ordersgen.OrderItem
	if err := json.Unmarshal(itemsRaw, &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Sku != "widget" {
		t.Fatalf("jsonb items column wrong: %s", itemsRaw)
	}

	// filters compile to the typed column, no jsonb cast
	rows, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{
		Namespace: "default",
		Filters:   []loom.Filter{{Field: "total_cents", Op: "gte", Value: "1000"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != dear.String() {
		t.Fatalf("typed filter wrong: %+v", rows)
	}

	// filtering a jsonb column is a loud error, not a silent mismatch
	if _, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{
		Namespace: "default",
		Filters:   []loom.Filter{{Field: "items", Op: "eq", Value: "x"}},
	}); err == nil || !strings.Contains(err.Error(), "not a scalar") {
		t.Fatalf("expected scalar-column error, got %v", err)
	}

	// typed reads still decode through the same doc-shaped path
	entity, err := cli.Entity(ctx, "OrderSummary", "default", cheap)
	if err != nil || entity == nil {
		t.Fatalf("Entity read: %v %v", entity, err)
	}
	if got := entity.(*ordersgen.OrderSummary).TotalCents; got != 500 {
		t.Fatalf("Entity decode wrong: %d", got)
	}

	// rebuild truncates the table and refolds the log into it
	if err := cli.Rebuild(ctx, "orderSummary"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "summaries refolded after rebuild", func() bool {
		rows, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{Namespace: "default"})
		return err == nil && len(rows) == 2
	})

	// declarative diff: a dropped column comes back on Migrate…
	if _, err := pool.Exec(ctx, `ALTER TABLE loom_t_orders_order_summary DROP COLUMN "currency"`); err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var typ string
	if err := pool.QueryRow(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_name='loom_t_orders_order_summary' AND column_name='currency'`).Scan(&typ); err != nil {
		t.Fatalf("currency column not re-added: %v", err)
	}
	if typ != "text" {
		t.Fatalf("re-added column has type %s", typ)
	}

	// …but type drift is an error with the rebuild remediation, never an
	// ALTER TYPE behind your back
	if _, err := pool.Exec(ctx, `ALTER TABLE loom_t_orders_order_summary DROP COLUMN "currency", ADD COLUMN "currency" bigint`); err != nil {
		t.Fatal(err)
	}
	err = cli.Migrate(ctx)
	if err == nil || !strings.Contains(err.Error(), "drift") {
		t.Fatalf("expected drift error, got %v", err)
	}
}
