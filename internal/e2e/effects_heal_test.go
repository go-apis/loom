package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
)

// plantInDoubt pays an invoice before any runner starts and leaves the
// capture claimed-but-unsettled, exactly as claimEffect leaves the row when
// the process dies before fn's outcome is recorded. Returns the scope.
func plantInDoubt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cli *loom.Client) (uuid.UUID, string) {
	t.Helper()
	billing.Gateway.Reset()
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)
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
	// the crashed instance had been running, so it also left a checkpoint
	// just short of the event: without it this is a brand-new process that
	// starts at the head (@from(head)) and never reacts to the event.
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq, updated_at)
		VALUES ('billing', 'process:captureOnPaid', $1, now())`, seq-1); err != nil {
		t.Fatal(err)
	}
	return invoice, scope
}

func deadLetterCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters WHERE service='billing'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func onlyEffect(t *testing.T, ctx context.Context, cli *loom.Client) loom.EffectRecord {
	t.Helper()
	effects, err := cli.Effects(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 1 {
		t.Fatalf("journal rows: %+v", effects)
	}
	return effects[0]
}

// TestIdempotentEffectReruns proves a crash between claim and settle on an
// @idempotent effect converges with no operator step: the next Once call
// re-runs the (declared repeatable) call and the reaction completes.
func TestIdempotentEffectReruns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	// what `effect gateway_capture @idempotent` generates (gen's own test
	// covers the schema → ReactorDef path)
	reg := billing.NewRegistry()
	for _, p := range reg.Processes {
		if p.Name == "captureOnPaid" {
			p.IdempotentEffects = []string{"gateway_capture"}
		}
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: reg, Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	invoice, _ := plantInDoubt(t, ctx, pool, cli)

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "idempotent capture to re-run", func() bool {
		return billing.LastReceipt() == "cap_"+invoice.String()
	})
	if n := billing.Gateway.CallsN(); n != 1 {
		t.Fatalf("gateway called %d times, want the one re-run", n)
	}
	fx := onlyEffect(t, ctx, cli)
	if fx.Status != "done" || fx.Attempts != 2 {
		t.Fatalf("effect after re-run: %+v", fx)
	}
	if n := deadLetterCount(t, ctx, pool); n != 0 {
		t.Fatalf("idempotent effect parked %d dead letters", n)
	}
}

// TestReconcileExecuted proves a Reconcile hook that finds the call did
// execute settles the effect done with its result, and Once returns that
// result — the gateway is never called again.
func TestReconcileExecuted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	var asked []string
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t),
		Reconcile: map[string]loom.ReconcileFunc{
			"gateway_capture": func(ctx context.Context, scope, key string) (json.RawMessage, bool, error) {
				asked = append(asked, scope+" "+key)
				return json.RawMessage(`"cap_reconciled"`), true, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_, scope := plantInDoubt(t, ctx, pool, cli)

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "reconciled receipt", func() bool {
		return billing.LastReceipt() == "cap_reconciled"
	})
	if n := billing.Gateway.CallsN(); n != 0 {
		t.Fatalf("gateway called %d times on a reconciled-as-executed effect", n)
	}
	if len(asked) != 1 || asked[0] != scope+" gateway_capture" {
		t.Fatalf("hook asked: %v", asked)
	}
	fx := onlyEffect(t, ctx, cli)
	if fx.Status != "done" || string(fx.Result) != `"cap_reconciled"` || fx.SettledAt == nil {
		t.Fatalf("effect after reconcile: %+v", fx)
	}
	if n := deadLetterCount(t, ctx, pool); n != 0 {
		t.Fatalf("reconciled effect parked %d dead letters", n)
	}
}

// TestReconcileNotExecuted proves a hook that finds the call never
// happened re-runs it, and a hook that cannot tell leaves the effect in
// doubt for an operator.
func TestReconcileNotExecuted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t),
		Reconcile: map[string]loom.ReconcileFunc{
			"gateway_capture": func(ctx context.Context, scope, key string) (json.RawMessage, bool, error) {
				return nil, false, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	invoice, _ := plantInDoubt(t, ctx, pool, cli)

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "re-run capture", func() bool {
		return billing.LastReceipt() == "cap_"+invoice.String()
	})
	if n := billing.Gateway.CallsN(); n != 1 {
		t.Fatalf("gateway called %d times, want the one re-run", n)
	}
	if fx := onlyEffect(t, ctx, cli); fx.Status != "done" || fx.Attempts != 2 {
		t.Fatalf("effect after re-run: %+v", fx)
	}
}

func TestReconcileErrStaysInDoubt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t),
		Reconcile: map[string]loom.ReconcileFunc{
			"gateway_capture": func(ctx context.Context, scope, key string) (json.RawMessage, bool, error) {
				return nil, false, errors.New("provider lookup unavailable")
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	plantInDoubt(t, ctx, pool, cli)

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "in-doubt reaction to park", func() bool {
		return deadLetterCount(t, ctx, pool) == 1
	})
	letters, err := cli.DeadLetters(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(letters) != 1 || !strings.Contains(letters[0].Error, "in doubt") || !strings.Contains(letters[0].Error, "provider lookup unavailable") {
		t.Fatalf("dead letters: %+v", letters)
	}
	if n := billing.Gateway.CallsN(); n != 0 {
		t.Fatalf("gateway called %d times on an unreconciled in-doubt effect", n)
	}
}

// TestReconcileUndeclaredEffect: a hook for an effect no process declares
// is a wiring mistake, caught at New.
func TestReconcileUndeclaredEffect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	_, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t),
		Reconcile: map[string]loom.ReconcileFunc{
			"gateway_refund": func(ctx context.Context, scope, key string) (json.RawMessage, bool, error) {
				return nil, false, nil
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "gateway_refund") {
		t.Fatalf("New with an undeclared reconcile hook: %v", err)
	}
}

// TestResolveWithRedrive proves the operator loop is one call: resolve
// with redrive: true settles the in-doubt effect and re-runs the reaction
// it parked, and the dead letter is gone.
func TestResolveWithRedrive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_, scope := plantInDoubt(t, ctx, pool, cli)

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()

	waitFor(t, ctx, "in-doubt reaction to park", func() bool {
		return deadLetterCount(t, ctx, pool) == 1
	})
	var letterID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM loom_dead_letters WHERE service='billing'`).Scan(&letterID); err != nil {
		t.Fatal(err)
	}

	resolve := fmt.Sprintf(`{"scope": %q, "key": "gateway_capture", "result": "cap_manual", "redrive": true}`, scope)
	resp, err := http.Post(srv.URL+"/effects/resolve", "application/json", strings.NewReader(resolve))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Status   string `json:"status"`
		Redriven int64  `json:"redriven"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || out.Status != "resolved" || out.Redriven != letterID {
		t.Fatalf("resolve with redrive: %d %+v (letter %d)", resp.StatusCode, out, letterID)
	}

	if billing.LastReceipt() != "cap_manual" || billing.Gateway.CallsN() != 0 {
		t.Fatalf("after resolve+redrive: receipt=%q calls=%d", billing.LastReceipt(), billing.Gateway.CallsN())
	}
	if n := deadLetterCount(t, ctx, pool); n != 0 {
		t.Fatalf("dead letter not cleared by resolve+redrive: %d", n)
	}
	if fx := onlyEffect(t, ctx, cli); fx.Status != "done" || string(fx.Result) != `"cap_manual"` {
		t.Fatalf("effect after resolve: %+v", fx)
	}
}
