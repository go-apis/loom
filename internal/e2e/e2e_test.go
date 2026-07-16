package e2e_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestOrderBillingLoop drives the full cross-service flow on a real
// Postgres and the in-memory bus:
//
//	PlaceOrder -> OrderPlaced (published)
//	  -> billing raiseOnOrder -> RaiseInvoice
//	MarkInvoicePaid -> InvoicePaid (published)
//	  -> orders shipOnPayment -> ShipOrder
//
// and asserts state, the read model, correlation propagation across the
// bus, and consumer dedup.
func TestOrderBillingLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	bus := loom.NewMemoryBus()

	ordersCli, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: orders.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	billingCli, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: billing.NewRegistry()})
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

	orderID := uuid.New()
	err = ordersCli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
		CustomerId:  uuid.New(),
		Currency:    "USD",
		Items: []ordersgen.OrderItem{
			{Sku: "widget", Quantity: 3, PriceCents: 500},
			{Sku: "gadget", Quantity: 1, PriceCents: 2500},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// billing reacts off the bus: invoice raised for 3*500 + 2500
	waitFor(t, ctx, "invoice raised", func() bool {
		state, _, err := billingCli.Load(ctx, "Invoice", "default", orderID)
		if err != nil {
			return false
		}
		inv := state.(*billinggen.Invoice)
		return inv.Status == "raised" && inv.AmountCents == 4000
	})

	if err := billingCli.Dispatch(ctx, &billinggen.MarkInvoicePaid{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}

	// orders reacts off the bus: shipped
	waitFor(t, ctx, "order shipped", func() bool {
		state, _, err := ordersCli.Load(ctx, "Order", "default", orderID)
		if err != nil {
			return false
		}
		return state.(*ordersgen.Order).Status == "shipped"
	})

	// the read model catches up
	waitFor(t, ctx, "order summary projected", func() bool {
		entity, err := ordersCli.Entity(ctx, "OrderSummary", "default", orderID)
		if err != nil || entity == nil {
			return false
		}
		summary := entity.(*ordersgen.OrderSummary)
		return summary.Status == "shipped" && summary.TotalCents == 4000
	})

	// correlation ids crossed the bus intact, both directions
	assertSameCorrelation(t, ctx, pool, "orders", "OrderPlaced", "billing", "InvoiceRaised")
	assertSameCorrelation(t, ctx, pool, "billing", "InvoicePaid", "orders", "OrderShipped")

	// duplicate delivery: re-publish the OrderPlaced envelope; dedup keeps
	// the invoice at version 1
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT envelope FROM loom_outbox WHERE service='orders' ORDER BY id LIMIT 1`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var env loom.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, &env); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_, version, err := billingCli.Load(ctx, "Invoice", "default", orderID)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 { // InvoiceRaised + InvoicePaid, no duplicate raise
		t.Fatalf("expected invoice at version 2 after duplicate delivery, got %d", version)
	}

	// nothing got parked anywhere
	var parked int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters`).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	if parked != 0 {
		t.Fatalf("expected no dead letters, got %d", parked)
	}
}

// TestProjectionRebuild wipes and refolds the read model from the log.
func TestProjectionRebuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	orderID := uuid.New()
	err = cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
		CustomerId:  uuid.New(),
		Currency:    "USD",
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 2, PriceCents: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "summary projected", func() bool {
		entity, err := cli.Entity(ctx, "OrderSummary", "default", orderID)
		return err == nil && entity != nil
	})

	if err := cli.Rebuild(ctx, "orderSummary"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "summary reprojected after rebuild", func() bool {
		entity, err := cli.Entity(ctx, "OrderSummary", "default", orderID)
		if err != nil || entity == nil {
			return false
		}
		return entity.(*ordersgen.OrderSummary).TotalCents == 200
	})
}

func testDB(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	admin := os.Getenv("LOOM_TEST_PG")
	if admin == "" {
		admin = "postgres://postgres:mysecret@localhost:5432/postgres"
	}
	adminPool, err := pgxpool.New(ctx, admin)
	if err != nil {
		t.Skipf("no postgres available: %v", err)
	}
	if err := adminPool.Ping(ctx); err != nil {
		t.Skipf("no postgres available: %v", err)
	}
	name := "loom_e2e_" + uuid.NewString()[:8]
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adminPool.Exec(cleanCtx, "DROP DATABASE "+name+" WITH (FORCE)")
		adminPool.Close()
	})

	cfg, err := pgxpool.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func waitFor(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func assertSameCorrelation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, svcA, typeA, svcB, typeB string) {
	t.Helper()
	var a, b string
	q := `SELECT correlation_id FROM loom_events WHERE service=$1 AND type=$2 ORDER BY global_seq LIMIT 1`
	if err := pool.QueryRow(ctx, q, svcA, typeA).Scan(&a); err != nil {
		t.Fatalf("correlation of %s/%s: %v", svcA, typeA, err)
	}
	if err := pool.QueryRow(ctx, q, svcB, typeB).Scan(&b); err != nil {
		t.Fatalf("correlation of %s/%s: %v", svcB, typeB, err)
	}
	if a == "" || a != b {
		t.Fatalf("correlation broke crossing the bus: %s/%s=%q vs %s/%s=%q", svcA, typeA, a, svcB, typeB, b)
	}
}
