package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// the index orders.loom declares on OrderSummary, under its default name
const (
	summaryIndex     = "idx_order_summary_service_namespace_customer_id_updated_at"
	summaryIndexCols = `(service, namespace, customer_id, updated_at DESC NULLS LAST)`
)

// summaryIndexState reads how many indexes carry the declared name, the
// definition of the one there is, and whether pg_index marks it valid.
func summaryIndexState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (n int, def string, valid bool) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT p.indexdef, i.indisvalid FROM pg_indexes p
		JOIN pg_class c ON c.relname = p.indexname
		JOIN pg_namespace ns ON ns.oid = c.relnamespace AND ns.nspname = p.schemaname
		JOIN pg_index i ON i.indexrelid = c.oid
		WHERE p.schemaname = current_schema() AND p.tablename = 'loom_t_orders_order_summary'
		  AND p.indexname = $1`, summaryIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		n++
		if err := rows.Scan(&def, &valid); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return n, def, valid
}

func requireSummaryIndex(t *testing.T, ctx context.Context, pool *pgxpool.Pool, when string) {
	t.Helper()
	n, def, valid := summaryIndexState(t, ctx, pool)
	if n != 1 {
		t.Fatalf("%s: %d indexes named %s, want 1", when, n, summaryIndex)
	}
	if !strings.HasSuffix(def, summaryIndexCols) || strings.Contains(def, "UNIQUE") {
		t.Fatalf("%s: index is %q, want the declared %s", when, def, summaryIndexCols)
	}
	if !valid {
		t.Fatalf("%s: index %s is not valid", when, summaryIndex)
	}
}

// TestMigrateCreatesDeclaredIndexes: Migrate creates a @table entity's
// declared index (CONCURRENTLY, outside the migration's transaction), a
// second Migrate is a no-op, and an invalid index of the same name — what
// a failed CREATE INDEX CONCURRENTLY leaves behind — is replaced by a
// valid one rather than skipped by IF NOT EXISTS.
func TestMigrateCreatesDeclaredIndexes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	requireSummaryIndex(t, ctx, pool, "first Migrate")

	if err := cli.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	requireSummaryIndex(t, ctx, pool, "second Migrate")

	// plant an invalid index under the declared name: a UNIQUE build over
	// duplicate rows fails CONCURRENTLY and leaves indisvalid = false
	if _, err := pool.Exec(ctx, `DROP INDEX `+summaryIndex); err != nil {
		t.Fatal(err)
	}
	customer := uuid.New()
	for i := 0; i < 2; i++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO loom_t_orders_order_summary (service, namespace, id, customer_id)
			VALUES ('orders', 'default', $1, $2)`, uuid.New(), customer); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY `+summaryIndex+
		` ON loom_t_orders_order_summary (customer_id)`); err == nil {
		t.Fatal("planting the invalid index: the unique build succeeded")
	}
	if n, _, valid := summaryIndexState(t, ctx, pool); n != 1 || valid {
		t.Fatalf("planted index: n=%d valid=%v, want one invalid", n, valid)
	}

	if err := cli.Migrate(ctx); err != nil {
		t.Fatalf("Migrate over an invalid index: %v", err)
	}
	requireSummaryIndex(t, ctx, pool, "Migrate over an invalid index")
}

// TestRebuildKeepsIndexes: Rebuild empties a @table read model with a
// service-scoped DELETE, so its declared index outlives the rebuild.
func TestRebuildKeepsIndexes(t *testing.T) {
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
	requireSummaryIndex(t, ctx, pool, "Migrate")

	orderID := uuid.New()
	if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
		CustomerId:  uuid.New(),
		Currency:    "USD",
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 500}},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "@table row folded", func() bool {
		row, _ := cli.Entity(ctx, "OrderSummary", "default", orderID)
		return row != nil
	})

	if err := cli.Rebuild(ctx, "orderSummary"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "@table row refolded after Rebuild", func() bool {
		row, _ := cli.Entity(ctx, "OrderSummary", "default", orderID)
		return row != nil
	})
	requireSummaryIndex(t, ctx, pool, "Rebuild")
}
