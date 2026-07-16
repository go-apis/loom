package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// TestEffectJournal proves call-once discipline: the gateway capture runs
// exactly once no matter how often the reaction around it retries, and a
// failed call (the handler asserting it did not happen) re-runs.
func TestEffectJournal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// the reaction fails twice AFTER capturing; retries must replay the
	// journaled receipt, not charge again
	billing.Gateway.Reset()
	billing.LastReceipt = ""
	billing.FailReactAfterCapture = 2

	invoice := uuid.New()
	payInvoice(t, ctx, cli, invoice)

	waitFor(t, ctx, "capture receipt", func() bool {
		return billing.LastReceipt == "cap_"+invoice.String()
	})
	if billing.Gateway.Calls != 1 {
		t.Fatalf("gateway called %d times, journal should have replayed", billing.Gateway.Calls)
	}
	effects, err := cli.Effects(ctx, "done", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 1 || effects[0].Key != "gateway_capture" || effects[0].Status != "done" {
		t.Fatalf("journal: %+v", effects)
	}
	var receipt string
	if err := json.Unmarshal(effects[0].Result, &receipt); err != nil || receipt != "cap_"+invoice.String() {
		t.Fatalf("journaled result: %s (%v)", effects[0].Result, err)
	}

	// a capture that errors re-runs on retry: two calls, then done
	billing.Gateway.Reset()
	billing.Gateway.FailCalls = 1
	invoice2 := uuid.New()
	payInvoice(t, ctx, cli, invoice2)

	waitFor(t, ctx, "second capture receipt", func() bool {
		return billing.LastReceipt == "cap_"+invoice2.String()
	})
	if billing.Gateway.Calls != 2 {
		t.Fatalf("gateway called %d times, want failed-then-retried = 2", billing.Gateway.Calls)
	}

	var parked int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters`).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	if parked != 0 {
		t.Fatalf("expected no dead letters, got %d", parked)
	}
}

// TestEffectInDoubt simulates a crash between claim and settle: the runtime
// must refuse to re-run the call and park the reaction, and the operator
// loop (resolve over HTTP, then redrive the dead letter) must finish the
// story without touching the gateway.
func TestEffectInDoubt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// events land in the log before any runner starts, so we can plant the
	// unsettled claim a crashed instance would have left
	billing.Gateway.Reset()
	billing.LastReceipt = ""
	billing.FailReactAfterCapture = 0
	invoice := uuid.New()
	payInvoice(t, ctx, cli, invoice)

	var seq int64
	if err := pool.QueryRow(ctx, `SELECT global_seq FROM loom_events WHERE service='billing' AND type='InvoicePaid'`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	scope := fmt.Sprintf("process:captureOnPaid/billing:%d", seq)
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_effects (service, scope, key, status) VALUES ('billing', $1, 'gateway_capture', 'running')`,
		scope); err != nil {
		t.Fatal(err)
	}

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()

	// the reaction must park, not re-charge
	waitFor(t, ctx, "in-doubt reaction to park", func() bool {
		var parked int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters WHERE service='billing'`).Scan(&parked)
		return parked == 1
	})
	if billing.Gateway.Calls != 0 {
		t.Fatalf("gateway called %d times on an in-doubt effect", billing.Gateway.Calls)
	}

	var letters struct {
		Items []loom.DeadLetter `json:"items"`
	}
	getJSON(t, ctx, srv.URL+"/dead_letters", &letters)
	if len(letters.Items) != 1 || !strings.Contains(letters.Items[0].Error, "in doubt") {
		t.Fatalf("dead letters: %+v", letters.Items)
	}

	// the journal shows the doubt
	var doubt struct {
		Items []loom.EffectRecord `json:"items"`
	}
	getJSON(t, ctx, srv.URL+"/effects?status=running", &doubt)
	if len(doubt.Items) != 1 || doubt.Items[0].Scope != scope {
		t.Fatalf("effects: %+v", doubt.Items)
	}

	// operator checked with the provider: the charge DID go through
	resolve := fmt.Sprintf(`{"scope": %q, "key": "gateway_capture", "result": "cap_manual"}`, scope)
	resp, err := http.Post(srv.URL+"/effects/resolve", "application/json", strings.NewReader(resolve))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resolve: %d", resp.StatusCode)
	}
	// resolving twice is refused — the effect is no longer in doubt
	resp, _ = http.Post(srv.URL+"/effects/resolve", "application/json", strings.NewReader(resolve))
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second resolve: %d", resp.StatusCode)
	}

	// redrive: the reaction replays the resolved receipt, gateway untouched
	resp, err = http.Post(fmt.Sprintf("%s/dead_letters/%d/redrive", srv.URL, letters.Items[0].ID), "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("redrive: %d", resp.StatusCode)
	}
	if billing.LastReceipt != "cap_manual" || billing.Gateway.Calls != 0 {
		t.Fatalf("after redrive: receipt=%q calls=%d", billing.LastReceipt, billing.Gateway.Calls)
	}
	var parked int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters WHERE service='billing'`).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	if parked != 0 {
		t.Fatalf("dead letter not cleared after redrive: %d", parked)
	}
}

// payInvoice raises and immediately pays an invoice, producing the local
// InvoicePaid event captureOnPaid reacts to.
func payInvoice(t *testing.T, ctx context.Context, cli *loom.Client, invoice uuid.UUID) {
	t.Helper()
	err := cli.Dispatch(ctx, &billinggen.RaiseInvoice{
		CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "default"},
		CustomerId:  uuid.New(),
		AmountCents: 4200,
		Currency:    "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Dispatch(ctx, &billinggen.MarkInvoicePaid{
		CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}
}
