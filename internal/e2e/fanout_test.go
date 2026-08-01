package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestConcurrentDispatchNoSkips is the regression test for the skipped-
// event gap: global_seq is assigned at INSERT, so without the service-
// wide append lock a later-seq transaction committing first let runners
// advance their checkpoints past a still-invisible earlier event —
// which was then never projected. Hammer dispatches concurrently across
// distinct aggregates and require the projection to see every one.
func TestConcurrentDispatchNoSkips(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- cli.Dispatch(ctx, &ordersgen.PlaceOrder{
				CommandBase: loom.CommandBase{AggregateID: uuid.New(), Namespace: "default"},
				CustomerId:  uuid.New(),
				Currency:    "USD",
				Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 100}},
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	// every order must land in the read model — a single miss is the bug
	waitFor(t, ctx, "all orders projected", func() bool {
		rows, err := cli.QueryEntities(ctx, "OrderSummary", loom.Query{Namespace: "default", Limit: 500})
		return err == nil && len(rows) == n
	})
}
