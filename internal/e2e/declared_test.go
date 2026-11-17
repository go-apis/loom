package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestAnEventNothingEmitsStillFolds proves an aggregate keeps folding an
// event it declares after the last command that emitted it is gone (ADR
// 0016). OrderDispatched is declared in Order and no command emits it: an
// older ShipOrder did. A stream that older binary wrote, OrderPlaced then
// OrderDispatched, replays from the store to a shipped order, and the
// handlers decide on that state: a cancel is refused and a ship
// converges. Without the fold the order would replay as placed, and the
// cancel would go through.
func TestAnEventNothingEmitsStillFolds(t *testing.T) {
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

	// the stream as the older binary wrote it
	order := uuid.New()
	mustExec(t, ctx, pool, `
		INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, schema_version, data)
		VALUES ('orders','default','Order',$1,1,'OrderPlaced',1,
		        '{"status":"placed","customer_id":"`+uuid.NewString()+`","items":[],"total_cents":700,"currency":"USD"}'),
		       ('orders','default','Order',$1,2,'OrderDispatched',1,'{"status":"shipped","shipped_at":"2026-11-01T09:30:00Z"}')`, order)

	state, version, err := cli.Load(ctx, "Order", "default", order)
	if err != nil {
		t.Fatal(err)
	}
	got := state.(*ordersgen.Order)
	shippedAt := time.Date(2026, 11, 1, 9, 30, 0, 0, time.UTC)
	if version != 2 || got.Status != ordersgen.OrderStatusShipped || got.ShippedAt == nil || !got.ShippedAt.Equal(shippedAt) {
		t.Fatalf("OrderDispatched did not fold: version=%d status=%q shipped_at=%v", version, got.Status, got.ShippedAt)
	}

	// the handlers see the folded state
	base := loom.CommandBase{AggregateID: order, Namespace: "default"}
	err = cli.Dispatch(ctx, &ordersgen.CancelOrder{CommandBase: base})
	if err == nil || !strings.Contains(err.Error(), `cannot cancel order in status "shipped"`) {
		t.Fatalf("cancelling the dispatched order: want it refused as shipped, got %v", err)
	}
	if err := cli.Dispatch(ctx, &ordersgen.ShipOrder{CommandBase: base}); err != nil {
		t.Fatalf("shipping the dispatched order again should converge: %v", err)
	}
	if _, version, err = cli.Load(ctx, "Order", "default", order); err != nil || version != 2 {
		t.Fatalf("a converged ship wrote to the stream: version=%d err=%v", version, err)
	}
}
