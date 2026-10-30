package e2e_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// TestHandlerReadsRunOnTheTransaction: a policy's loom.Load, GetRecord and
// GetEntity run on the dispatch's own transaction, never on the pool. The
// transaction holds the service's log lock from its first append, and
// every other dispatch waits on that lock holding a pooled connection: a
// policy read that went to the pool waited for a connection they held,
// and once they held every free one nothing moved. mesh-api hung that way
// on 2026-10-30, a hand-back's policy (ledgerOnLease) reading the Lease
// record on the pool with the log lock held. On the transaction the read
// also sees what the unit has written so far.
func TestHandlerReadsRunOnTheTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	// postLedger reacts to InvoicePaid in the payment's transaction; here
	// it also reads the invoice, a record and an entity first
	var mu sync.Mutex
	seen := map[uuid.UUID]string{}
	reading := func() *loom.Registry {
		reg := billing.NewRegistry()
		for _, p := range reg.Policies {
			if p.Name != "postLedger" {
				continue
			}
			react := p.React
			p.React = func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
				st, _, err := loom.Load(ctx, "Invoice", evt.Namespace, evt.AggregateID)
				if err != nil {
					return nil, err
				}
				if _, err := loom.GetRecord(ctx, "LedgerEntry", evt.Namespace, evt.AggregateID); err != nil {
					return nil, err
				}
				if _, err := loom.GetEntity(ctx, "PayeeDirectory", evt.Namespace, evt.AggregateID); err != nil {
					return nil, err
				}
				mu.Lock()
				seen[evt.AggregateID] = st.(*billinggen.Invoice).Status
				mu.Unlock()
				return react(ctx, evt)
			}
		}
		return reg
	}

	seed, err := loom.New(loom.Config{DB: pool, Registry: reading(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	invoices := make([]uuid.UUID, 13)
	for i := range invoices {
		invoices[i] = uuid.New()
		if err := seed.Dispatch(ctx, &billinggen.RaiseInvoice{
			CommandBase: loom.CommandBase{AggregateID: invoices[i], Namespace: "default"},
			CustomerId:  uuid.New(), AmountCents: 100, Currency: "USD",
		}); err != nil {
			t.Fatal(err)
		}
	}
	pay := func(ctx context.Context, c *loom.Client, id uuid.UUID) error {
		return c.Dispatch(ctx, &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: id, Namespace: "default"}})
	}
	tight := func(t *testing.T, maxConns int32) *loom.Client {
		t.Helper()
		cfg := pool.Config()
		cfg.MaxConns = maxConns
		cfg.MinConns = 0
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		c, err := loom.New(loom.Config{DB: p, Registry: reading(), Keys: testKeys(t)})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	// bounded: a deadlocked dispatch never returns on its own
	bounded := func(t *testing.T, what string, d time.Duration, f func(ctx context.Context) error) {
		t.Helper()
		sctx, scancel := context.WithTimeout(ctx, d)
		defer scancel()
		if err := f(sctx); err != nil {
			t.Fatalf("%s: %v (a policy read waited for a pooled connection inside the transaction)", what, err)
		}
	}

	t.Run("one connection", func(t *testing.T) {
		c := tight(t, 1)
		bounded(t, "a dispatch whose policy reads", 5*time.Second, func(ctx context.Context) error {
			return pay(ctx, c, invoices[0])
		})
		mu.Lock()
		defer mu.Unlock()
		// read on the transaction: the policy sees the InvoicePaid its
		// own unit appended (the pool saw it still raised)
		if seen[invoices[0]] != "paid" {
			t.Fatalf("the policy read the invoice as %q, want paid", seen[invoices[0]])
		}
	})

	t.Run("concurrent dispatches queued on the log lock", func(t *testing.T) {
		// three connections and twelve dispatches: the one with the log
		// lock reads while the others wait on it, a connection apiece
		c := tight(t, 3)
		bounded(t, "concurrent dispatches whose policies read", 15*time.Second, func(ctx context.Context) error {
			var wg sync.WaitGroup
			errs := make(chan error, len(invoices))
			for _, id := range invoices[1:] {
				wg.Add(1)
				go func() { defer wg.Done(); errs <- pay(ctx, c, id) }()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// TestLogLockWait: a dispatch waits at most Config.LogLockWait for the
// service's log lock, then fails with a *LogLockError naming the session
// that holds it, and gives its connection back. Unbounded, a unit of work
// stuck while holding the lock hung every dispatch behind it, each with a
// pooled connection, until the service stopped answering (mesh-api,
// 2026-10-30).
func TestLogLockWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	c, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t), LogLockWait: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	raise := func(ctx context.Context) error {
		return c.Dispatch(ctx, &billinggen.RaiseInvoice{
			CommandBase: loom.CommandBase{AggregateID: uuid.New(), Namespace: "default"},
			CustomerId:  uuid.New(), AmountCents: 100, Currency: "USD",
		})
	}

	// a stuck unit of work: holds the log lock, idle in its transaction
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx)
	var pid int
	if err := holder.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('loom_log_' || $1))`, "billing"); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err = raise(ctx)
	var stuck *loom.LogLockError
	if !errors.As(err, &stuck) {
		t.Fatalf("a dispatch behind a held log lock: %v, want a *loom.LogLockError", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the dispatch gave up after %s, want about 300ms", took)
	}
	if stuck.Holder.PID != pid || stuck.Holder.State != "idle in transaction" {
		t.Fatalf("holder %+v, want pid %d idle in transaction", stuck.Holder, pid)
	}
	t.Log(err)

	// its connection went back: the pool serves the next dispatch once
	// the lock is let go
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := raise(ctx); err != nil {
		t.Fatalf("a dispatch after the holder let go: %v", err)
	}
}
