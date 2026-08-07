package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestReset drives the factory wipe end to end: tables are emptied but
// never dropped — @table mirrors included, which are not loom_* named
// and would otherwise keep stale read-model rows — and the store stays
// usable without re-migration: a fresh dispatch folds cleanly after.
func TestReset(t *testing.T) {
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
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()

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

	// the wrong phrase refuses; the service name arms
	resp, err := http.Post(srv.URL+"/reset", "application/json", bytes.NewReader([]byte(`{"confirm":"nope"}`)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("wrong confirm: %d, want 400", resp.StatusCode)
	}
	resp, err = http.Post(srv.URL+"/reset", "application/json", bytes.NewReader([]byte(`{"confirm":"orders"}`)))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Tables []string `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("reset: %d", resp.StatusCode)
	}
	sawTable := false
	for _, tb := range out.Tables {
		if tb == "loom_t_orders_order_summary" {
			sawTable = true
		}
	}
	if !sawTable {
		t.Fatalf("@table mirror missing from reset's table list: %v", out.Tables)
	}

	// everything is empty — the @table mirror included
	if entries, err := cli.QueryLog(ctx, loom.LogQuery{}); err != nil || len(entries) != 0 {
		t.Fatalf("log should be empty, got %d err=%v", len(entries), err)
	}
	if row, _ := cli.Entity(ctx, "OrderSummary", "default", orderID); row != nil {
		t.Fatal("@table row survived the reset")
	}

	// no re-migration needed: a fresh dispatch folds end to end
	fresh := uuid.New()
	if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: fresh, Namespace: "default"},
		CustomerId:  uuid.New(),
		Currency:    "USD",
		Items:       []ordersgen.OrderItem{{Sku: "gadget", Quantity: 1, PriceCents: 100}},
	}); err != nil {
		t.Fatalf("post-reset dispatch: %v", err)
	}
	waitFor(t, ctx, "post-reset fold", func() bool {
		row, _ := cli.Entity(ctx, "OrderSummary", "default", fresh)
		return row != nil
	})
}
