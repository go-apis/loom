package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// TestQueryEntitiesCursorPagesEveryRowOnce proves ADR 0014's fix: an
// OFFSET-paged list read could skip or repeat rows whenever the ORDER BY
// column (updated_at, by default) tied across a page boundary, because
// OFFSET only counts rows, and Postgres is free to order tied rows
// differently between two separately-planned queries. Keyset pagination
// tie-breaks ORDER BY on id and walks the WHERE predicate forward from a
// cursor instead, so every row is visited exactly once regardless of
// ties — even the worst case this test forces: every row tied on the
// order column at once.
func TestQueryEntitiesCursorPagesEveryRowOnce(t *testing.T) {
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

	const total = 250
	customer := uuid.New()
	ids := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		id := uuid.New()
		placeOrder(t, ctx, cli, id, customer, 100)
		ids[id.String()] = true
	}

	waitFor(t, ctx, "all 250 summaries projected", func() bool {
		rows, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{Namespace: "default", Limit: 500})
		return err == nil && len(rows) == total
	})

	// force the tie: every row gets the exact same updated_at, so the
	// default ORDER BY updated_at has nothing but the id tie-breaker left
	// to keep three Limit=100 pages from skipping or repeating a row
	tie := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `UPDATE loom_t_orders_order_summary SET updated_at=$1 WHERE service='orders' AND namespace='default'`, tie); err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	var pageSizes []int
	q := loom.Query{Namespace: "default", Limit: 100}
	for i := 0; i < 3; i++ {
		rows, err := cli.QueryEntities(ctx, "OrderSummary", q)
		if err != nil {
			t.Fatal(err)
		}
		pageSizes = append(pageSizes, len(rows))
		for _, r := range rows {
			seen[r.ID]++
		}
		if len(rows) == 0 {
			break
		}
		cursor, err := rows[len(rows)-1].Cursor(q.OrderBy)
		if err != nil {
			t.Fatal(err)
		}
		q.After = &cursor
	}

	if len(pageSizes) != 3 || pageSizes[0] != 100 || pageSizes[1] != 100 || pageSizes[2] != 50 {
		t.Fatalf("page sizes: %v", pageSizes)
	}
	if len(seen) != total {
		t.Fatalf("union of pages has %d distinct ids, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("id %s seen %d times", id, n)
		}
		if !ids[id] {
			t.Fatalf("id %s was never inserted", id)
		}
	}
}
