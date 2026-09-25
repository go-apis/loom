package e2e_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// retryingBilling starts billing with captureOnPaid declaring what
// `process captureOnPaid @retry(5, 100ms..500ms)` generates (gen's own
// test covers the schema → ReactorDef path).
func retryingBilling(t *testing.T, ctx context.Context) (*pgxpool.Pool, *loom.Client) {
	t.Helper()
	return retryingBillingWith(t, ctx, &loom.RetryPolicy{Max: 5, Min: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond})
}

func retryingBillingWith(t *testing.T, ctx context.Context, policy *loom.RetryPolicy) (*pgxpool.Pool, *loom.Client) {
	t.Helper()
	return retryingBillingWrapped(t, ctx, policy, nil)
}

// retryingBillingWrapped is retryingBillingWith with captureOnPaid's
// reaction wrapped, the harness pool handed to the wrapper.
func retryingBillingWrapped(t *testing.T, ctx context.Context, policy *loom.RetryPolicy,
	wrap func(pool *pgxpool.Pool, react func(context.Context, *loom.Event) ([]loom.Command, error)) func(context.Context, *loom.Event) ([]loom.Command, error),
) (*pgxpool.Pool, *loom.Client) {
	t.Helper()
	pool := testDB(t, ctx)
	reg := billing.NewRegistry()
	for _, p := range reg.Processes {
		if p.Name == "captureOnPaid" {
			p.Retry = policy
			if wrap != nil {
				p.React = wrap(pool, p.React)
			}
		}
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: reg, Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	billing.Gateway.Reset()
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)
	return pool, cli
}

func retryRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM loom_timers WHERE service='billing' AND command_type='loom:retry'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestRetryConverges proves a transient failure that outlasts the
// immediate in-process attempts is re-armed as a durable timer instead of
// parking, and converges with no dead letter ever written.
func TestRetryConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, cli := retryingBilling(t, ctx)

	// three immediate attempts fail, then the first two durable ones
	billing.Gateway.SetFailCalls(5)
	invoice := uuid.New()
	payInvoice(t, ctx, cli, invoice)
	id := invoice.String()

	sawRetry := false
	waitFor(t, ctx, "capture to converge through durable retries", func() bool {
		if retryRows(t, ctx, pool) > 0 {
			sawRetry = true
		}
		if deadLetterCount(t, ctx, pool) != 0 {
			t.Fatalf("a @retry reaction parked before max")
		}
		return billing.LastReceipt() == "cap_"+id
	})
	if !sawRetry {
		t.Fatalf("no durable retry row was ever visible on loom_timers")
	}
	if n := billing.Gateway.CallsN(); n != 6 {
		t.Fatalf("gateway called %d times, want 3 immediate + 3 durable = 6", n)
	}
	waitFor(t, ctx, "retry row to clear", func() bool { return retryRows(t, ctx, pool) == 0 })
	if n := deadLetterCount(t, ctx, pool); n != 0 {
		t.Fatalf("converged reaction left %d dead letters", n)
	}
	fx := onlyEffect(t, ctx, cli)
	if fx.Status != "done" {
		t.Fatalf("effect after durable retries: %+v", fx)
	}
}

// TestRetryExhaustedParks proves a @retry reaction failing through every
// durable attempt still parks to dead letters exactly once, as an
// undeclared process would — only later, with every attempt counted.
func TestRetryExhaustedParks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, cli := retryingBilling(t, ctx)

	billing.Gateway.SetFailCalls(1000)
	payInvoice(t, ctx, cli, uuid.New())

	waitFor(t, ctx, "exhausted retry to park", func() bool { return deadLetterCount(t, ctx, pool) > 0 })
	letters, err := cli.DeadLetters(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(letters) != 1 || letters[0].Runner != "process:captureOnPaid" || letters[0].Attempts != 3+5 {
		t.Fatalf("dead letters: %+v", letters)
	}
	var parked struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(letters[0].Envelope, &parked); err != nil || parked.Type != "InvoicePaid" {
		t.Fatalf("parked envelope: %s (%v)", letters[0].Envelope, err)
	}
	if n := billing.Gateway.CallsN(); n != 3+5 {
		t.Fatalf("gateway called %d times, want 3 immediate + 5 durable = 8", n)
	}
	if n := retryRows(t, ctx, pool); n != 0 {
		t.Fatalf("%d retry rows outlived the park", n)
	}
	// and nothing re-arms or parks twice afterwards
	time.Sleep(700 * time.Millisecond)
	if n := deadLetterCount(t, ctx, pool); n != 1 {
		t.Fatalf("dead letters after settling: %d", n)
	}
}

// TestRetryRedeliveryConverges proves a redelivery of an event a durable
// retry already owns (here: the checkpoint rewound past it, as a crash
// before the checkpoint write would) converges on that retry — no second
// round of reactions, no second row.
func TestRetryRedeliveryConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// a long backoff keeps the retry pending while the event is redelivered
	pool, cli := retryingBillingWith(t, ctx, &loom.RetryPolicy{Max: 1, Min: time.Minute, MaxBackoff: time.Minute})

	billing.Gateway.SetFailCalls(1000)
	payInvoice(t, ctx, cli, uuid.New())
	waitFor(t, ctx, "durable retry to arm", func() bool { return retryRows(t, ctx, pool) == 1 })
	if n := billing.Gateway.CallsN(); n != 3 {
		t.Fatalf("gateway called %d times before arming, want 3", n)
	}

	var seq int64
	if err := pool.QueryRow(ctx, `SELECT global_seq FROM loom_events WHERE service='billing' AND type='InvoicePaid'`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE loom_checkpoints SET global_seq=$1 WHERE service='billing' AND runner='process:captureOnPaid'`, seq-1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "checkpoint to pass the event again", func() bool {
		var at int64
		_ = pool.QueryRow(ctx, `SELECT global_seq FROM loom_checkpoints WHERE service='billing' AND runner='process:captureOnPaid'`).Scan(&at)
		return at >= seq
	})
	if n := billing.Gateway.CallsN(); n != 3 {
		t.Fatalf("redelivery reacted again: gateway called %d times, want 3", n)
	}
	if n := retryRows(t, ctx, pool); n != 1 {
		t.Fatalf("retry rows after redelivery: %d", n)
	}
	if n := deadLetterCount(t, ctx, pool); n != 0 {
		t.Fatalf("redelivery parked %d dead letters", n)
	}
}

// TestRetryReactionRearmsConverges proves a durable retry's reaction runs
// with no claim transaction open (docs/adr/0002): while the successful
// durable attempt reacts, another connection can lock the retry row with
// NOWAIT, and the timer the reaction re-arms is written straight through.
// The retry then clears conditionally and nothing hangs or parks.
func TestRetryReactionRearmsConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var mu sync.Mutex
	var probes []error
	wrap := func(pool *pgxpool.Pool, react func(context.Context, *loom.Event) ([]loom.Command, error)) func(context.Context, *loom.Event) ([]loom.Command, error) {
		return func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
			cmds, err := react(ctx, evt)
			if err != nil {
				return nil, err
			}
			// a durable attempt: its retry row exists; it must not be locked
			tx, err := pool.Begin(ctx)
			if err != nil {
				return nil, err
			}
			defer tx.Rollback(ctx)
			var n int
			rows, err := tx.Query(ctx, `
				SELECT 1 FROM loom_timers WHERE service='billing' AND command_type='loom:retry'
				FOR UPDATE NOWAIT`)
			if err == nil {
				for rows.Next() {
					n++
				}
				rows.Close()
				err = rows.Err()
			}
			mu.Lock()
			if err != nil || n > 0 {
				probes = append(probes, err)
			}
			mu.Unlock()
			return append(cmds, loom.WithKey(loom.After(&billinggen.MarkInvoicePaid{
				CommandBase: loom.CommandBase{AggregateID: evt.AggregateID, Namespace: evt.Namespace},
			}, time.Hour), "rearm:"+evt.AggregateID.String())), nil
		}
	}
	pool, cli := retryingBillingWrapped(t, ctx, &loom.RetryPolicy{Max: 5, Min: 100 * time.Millisecond, MaxBackoff: 300 * time.Millisecond}, wrap)

	// the three immediate attempts fail; the first durable one succeeds
	billing.Gateway.SetFailCalls(3)
	invoice := uuid.New()
	payInvoice(t, ctx, cli, invoice)

	waitFor(t, ctx, "the durable retry to converge", func() bool {
		return billing.LastReceipt() == "cap_"+invoice.String()
	})
	waitFor(t, ctx, "retry row to clear", func() bool { return retryRows(t, ctx, pool) == 0 })
	var fireAt time.Time
	if err := pool.QueryRow(ctx, `SELECT fire_at FROM loom_timers WHERE service='billing' AND key=$1`,
		"rearm:"+invoice.String()).Scan(&fireAt); err != nil || time.Until(fireAt) < 50*time.Minute {
		t.Fatalf("re-armed timer: fire_at %v (%v)", fireAt, err)
	}
	if n := deadLetterCount(t, ctx, pool); n != 0 {
		t.Fatalf("converged reaction left %d dead letters", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(probes) != 1 || probes[0] != nil {
		t.Fatalf("durable attempt's retry row probes (want one, unlocked): %v", probes)
	}
}
