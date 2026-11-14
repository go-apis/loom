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

// TestDeleteStream is the junk lever end to end: delete one of two
// orders through POST /streams/delete and assert the surviving order is
// untouched while the deleted one is gone from every store — events,
// state, the id-keyed @table row — and that rebuild purges the re-keyed
// customerSpend row the id-based deletes cannot reach.
func TestDeleteStream(t *testing.T) {
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

	junk, keeper := uuid.New(), uuid.New()
	junkCustomer, keeperCustomer := uuid.New(), uuid.New()
	place := func(id, customer uuid.UUID) {
		t.Helper()
		if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
			CommandBase: loom.CommandBase{AggregateID: id, Namespace: "default"},
			CustomerId:  customer,
			Currency:    "USD",
			Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 500}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	place(junk, junkCustomer)
	place(keeper, keeperCustomer)

	waitFor(t, ctx, "read models folded", func() bool {
		a, _ := cli.Entity(ctx, "OrderSummary", "default", junk)
		b, _ := cli.Entity(ctx, "OrderSummary", "default", keeper)
		s, _ := cli.Entity(ctx, "CustomerSpend", "default", junkCustomer)
		return a != nil && b != nil && s != nil
	})

	// counted first: the preview says what the delete will remove and
	// what it will leave to rebuild, and removes nothing
	preview, err := cli.PreviewDeleteStream(ctx, "default", junk)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Events == 0 || preview.Rows == 0 || len(preview.Projections) == 0 {
		t.Fatalf("preview = %+v; want the stream's events, rows and projections", preview)
	}
	if _, version, err := cli.Load(ctx, "Order", "default", junk); err != nil || version == 0 {
		t.Fatalf("the preview deleted the stream: v%d err=%v", version, err)
	}
	if row, _ := cli.Entity(ctx, "OrderSummary", "default", junk); row == nil {
		t.Fatal("the preview deleted the stream's @table row")
	}
	dry, _ := json.Marshal(map[string]any{"namespace": "default", "ids": []uuid.UUID{junk}, "rebuild": true, "dry_run": true})
	dryResp, err := http.Post(srv.URL+"/streams/delete", "application/json", bytes.NewReader(dry))
	if err != nil {
		t.Fatal(err)
	}
	var dryOut struct {
		Status  string              `json:"status"`
		Deleted loom.StreamDeletion `json:"deleted"`
		Rebuilt []string            `json:"rebuilt"`
	}
	err = json.NewDecoder(dryResp.Body).Decode(&dryOut)
	dryResp.Body.Close()
	if err != nil || dryOut.Status != "previewed" || dryOut.Deleted.Events != preview.Events || len(dryOut.Rebuilt) != 0 {
		t.Fatalf("dry run over the API = %+v, %v; want the preview's counts and nothing rebuilt", dryOut, err)
	}
	if _, version, err := cli.Load(ctx, "Order", "default", junk); err != nil || version == 0 {
		t.Fatalf("the dry run deleted the stream: v%d err=%v", version, err)
	}

	body, _ := json.Marshal(map[string]any{
		"namespace": "default", "ids": []uuid.UUID{junk}, "rebuild": true,
	})
	resp, err := http.Post(srv.URL+"/streams/delete", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Deleted  loom.StreamDeletion `json:"deleted"`
		NotFound []uuid.UUID         `json:"not_found"`
		Rebuilt  []string            `json:"rebuilt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	if out.Deleted.Events == 0 || out.Deleted.Rows == 0 {
		t.Fatalf("expected events and rows deleted, got %+v", out.Deleted)
	}
	if out.Deleted.Events != preview.Events || out.Deleted.Rows != preview.Rows || out.Deleted.Snapshots != preview.Snapshots {
		t.Errorf("the delete removed %+v; the preview said %+v", out.Deleted, *preview)
	}
	if len(out.Rebuilt) == 0 {
		t.Fatalf("expected affected projections rebuilt, got %+v", out)
	}

	// the deleted stream is gone from the log and folds to nothing
	entries, err := cli.QueryLog(ctx, loom.LogQuery{AggregateID: junk.String()})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("deleted stream still has %d events", len(entries))
	}
	if _, version, err := cli.Load(ctx, "Order", "default", junk); err != nil || version != 0 {
		t.Fatalf("deleted stream should fold to version 0, got v%d err=%v", version, err)
	}
	if row, _ := cli.Entity(ctx, "OrderSummary", "default", junk); row != nil {
		t.Fatal("deleted stream's @table row survived")
	}

	// the keeper is untouched, and the rebuild purges the junk customer's
	// re-keyed spend row while refolding the keeper's
	if _, version, err := cli.Load(ctx, "Order", "default", keeper); err != nil || version == 0 {
		t.Fatalf("keeper stream damaged: v%d err=%v", version, err)
	}
	waitFor(t, ctx, "rebuild refolded the survivors", func() bool {
		gone, _ := cli.Entity(ctx, "CustomerSpend", "default", junkCustomer)
		kept, _ := cli.Entity(ctx, "CustomerSpend", "default", keeperCustomer)
		summary, _ := cli.Entity(ctx, "OrderSummary", "default", keeper)
		return gone == nil && kept != nil && summary != nil
	})

	// a second delete of the same stream converges to not_found
	resp, err = http.Post(srv.URL+"/streams/delete", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	out.NotFound = nil
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(out.NotFound) != 1 || out.NotFound[0] != junk {
		t.Fatalf("retried delete should report not_found, got %+v", out.NotFound)
	}
}
