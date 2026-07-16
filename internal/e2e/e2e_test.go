package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

	// the ledger record was posted in the payment's transaction
	rec, err := billingCli.Record(ctx, "LedgerEntry", "default", orderID)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("ledger entry record missing")
	}
	ledger := rec.(*billinggen.LedgerEntry)
	if ledger.AmountCents != 4000 || ledger.PostedAt.IsZero() {
		t.Fatalf("ledger entry wrong: %+v", ledger)
	}

	// shipping cancelled the auto-cancel timer
	var timers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_timers WHERE service='orders'`).Scan(&timers); err != nil {
		t.Fatal(err)
	}
	if timers != 0 {
		t.Fatalf("expected the auto-cancel timer to be cancelled, %d timers remain", timers)
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

// TestAutoCancelTimer proves durable timers fire: an order placed and never
// paid cancels itself.
func TestAutoCancelTimer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = 400 * time.Millisecond
	t.Cleanup(func() { orders.AutoCancelAfter = old })

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
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, ctx, "unpaid order to auto-cancel", func() bool {
		state, _, err := cli.Load(ctx, "Order", "default", orderID)
		if err != nil {
			return false
		}
		return state.(*ordersgen.Order).Status == "cancelled"
	})
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

// TestHTTPAPI drives the service purely over the registry-driven HTTP
// surface: dispatch a command, search the read model, browse the log by
// correlation, read ops stats.
func TestHTTPAPI(t *testing.T) {
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
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()

	orderID := uuid.New()
	body := fmt.Sprintf(`{
		"aggregate_id": %q, "namespace": "default",
		"customer_id": %q, "currency": "USD",
		"items": [{"sku": "widget", "quantity": 2, "price_cents": 750}]
	}`, orderID, uuid.New())
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/commands/PlaceOrder", strings.NewReader(body))
	req.Header.Set("X-Correlation-Id", "corr-http-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("dispatch: got %d", resp.StatusCode)
	}

	// unknown command 404s
	resp, _ = http.Post(srv.URL+"/commands/Nope", "application/json", strings.NewReader(`{}`))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown command: got %d", resp.StatusCode)
	}

	// aggregate state over HTTP
	var agg struct {
		Version int             `json:"version"`
		State   json.RawMessage `json:"state"`
	}
	getJSON(t, ctx, srv.URL+"/aggregates/Order/"+orderID.String()+"?namespace=default", &agg)
	if agg.Version != 1 {
		t.Fatalf("aggregate version: %d", agg.Version)
	}

	// filtered read-model search (wait for the projection)
	waitFor(t, ctx, "summary queryable over http", func() bool {
		var list struct {
			Items []loom.Row `json:"items"`
		}
		getJSON(t, ctx, srv.URL+"/entities/OrderSummary?namespace=default&status=placed&total_cents.gte=1000", &list)
		return len(list.Items) == 1 && list.Items[0].ID == orderID.String()
	})
	// a filter that should exclude it
	var none struct {
		Items []loom.Row `json:"items"`
	}
	getJSON(t, ctx, srv.URL+"/entities/OrderSummary?namespace=default&total_cents.gte=99999", &none)
	if len(none.Items) != 0 {
		t.Fatalf("filter failed to exclude: %d items", len(none.Items))
	}

	// log browsing by correlation
	var log struct {
		Items []loom.LogEntry `json:"items"`
	}
	getJSON(t, ctx, srv.URL+"/events?correlation_id=corr-http-test", &log)
	if len(log.Items) != 1 || log.Items[0].Type != "OrderPlaced" {
		t.Fatalf("log query: %+v", log.Items)
	}

	// ops stats
	var stats map[string]any
	getJSON(t, ctx, srv.URL+"/stats", &stats)
	if stats["service"] != "orders" {
		t.Fatalf("stats: %+v", stats)
	}
}

func getJSON(t *testing.T, ctx context.Context, url string, into any) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatal(err)
	}
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
