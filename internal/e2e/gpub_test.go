package e2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/gpub"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestPubSubBus runs the cross-service loop over real (emulated) Google
// Cloud Pub/Sub instead of the in-memory bus: OrderPlaced crosses to
// billing, InvoicePaid crosses back, correlation survives both hops, and
// broker redelivery is absorbed by consumer dedup.
func TestPubSubBus(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("PUBSUB_EMULATOR_HOST not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	topic := "loom-e2e-" + uuid.NewString()[:8]

	newBus := func() *gpub.Bus {
		b, err := gpub.New(ctx, gpub.Config{ProjectID: "loom-e2e", TopicID: topic})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	// each service holds its own client, like separate deployments
	ordersCli, err := loom.New(loom.Config{DB: pool, Bus: newBus(), Registry: orders.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	billingCli, err := loom.New(loom.Config{DB: pool, Bus: newBus(), Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ordersCli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ordersCli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := billingCli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	billing.Gateway.Reset()
	billing.FailReactAfterCapture = 0

	orderID := uuid.New()
	err = ordersCli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
		CustomerId:  uuid.New(),
		Currency:    "USD",
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 2, PriceCents: 1000}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// OrderPlaced crossed Pub/Sub into billing
	waitFor(t, ctx, "invoice raised over pub/sub", func() bool {
		state, _, err := billingCli.Load(ctx, "Invoice", "default", orderID)
		if err != nil {
			return false
		}
		inv := state.(*billinggen.Invoice)
		return inv.Status == "raised" && inv.AmountCents == 2000
	})

	if err := billingCli.Dispatch(ctx, &billinggen.MarkInvoicePaid{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	// InvoicePaid crossed back into orders
	waitFor(t, ctx, "order shipped over pub/sub", func() bool {
		state, _, err := ordersCli.Load(ctx, "Order", "default", orderID)
		if err != nil {
			return false
		}
		return state.(*ordersgen.Order).Status == "shipped"
	})

	// correlation survived both hops through the broker
	assertSameCorrelation(t, ctx, pool, "orders", "OrderPlaced", "billing", "InvoiceRaised")
	assertSameCorrelation(t, ctx, pool, "billing", "InvoicePaid", "orders", "OrderShipped")

	// nothing parked; the invoice saw exactly one raise despite an
	// at-least-once broker
	var parked int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters`).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	if parked != 0 {
		t.Fatalf("expected no dead letters, got %d", parked)
	}
	_, version, err := billingCli.Load(ctx, "Invoice", "default", orderID)
	if err != nil || version != 2 {
		t.Fatalf("invoice version %d (%v)", version, err)
	}
}
