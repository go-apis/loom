package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// TestSealedReadsHoldOneConnection: opening or sealing @pii fields never
// takes a second pooled connection while the caller holds one. Each
// client here starts with a cold data-key cache on a pool too small for a
// second connection, so the key read must share the connection (or
// transaction) the caller already has. Before the fix, readLog, Load and
// Entities decrypted with their rows still open and every write sealed
// through the pool from inside its transaction: on a full pool each of
// these waited forever for a connection it was itself holding.
func TestSealedReadsHoldOneConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	seed, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	payees := make([]uuid.UUID, 6)
	for i := range payees {
		payees[i] = uuid.New()
		if err := seed.Dispatch(ctx, &billinggen.RegisterPayee{
			CommandBase: loom.CommandBase{AggregateID: payees[i], Namespace: "default"},
			Name:        "Payee", Tin: "123-45-6789", TinLast4: "6789",
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Payee snapshots every event, and a snapshot is read with QueryRow
	// (no rows held). Without snapshots Load folds the sealed events
	// from an open rows cursor, the path that held its connection.
	if _, err := pool.Exec(ctx, `DELETE FROM loom_snapshots`); err != nil {
		t.Fatal(err)
	}

	tight := func(t *testing.T, maxConns int32) *pgxpool.Pool {
		t.Helper()
		cfg := pool.Config()
		cfg.MaxConns = maxConns
		cfg.MinConns = 0
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	// cold: a fresh client has an empty data-key cache, so the first open
	// or seal of each stream reads loom_keys
	cold := func(t *testing.T, p *pgxpool.Pool) *loom.Client {
		t.Helper()
		c, err := loom.New(loom.Config{DB: p, Registry: billing.NewRegistry(), Keys: testKeys(t)})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	// bounded: a deadlocked call never returns on its own, so each step
	// runs under a short deadline and failing to finish is the failure
	bounded := func(t *testing.T, what string, f func(ctx context.Context) error) {
		t.Helper()
		sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
		defer scancel()
		if err := f(sctx); err != nil {
			t.Fatalf("%s on a full pool: %v (a second pooled connection taken while holding one)", what, err)
		}
	}

	t.Run("Load folds sealed events on one connection", func(t *testing.T) {
		c := cold(t, tight(t, 1))
		bounded(t, "Load", func(ctx context.Context) error {
			state, _, err := c.Load(ctx, "Payee", "default", payees[0])
			if err == nil && state.(*billinggen.Payee).Tin != "123-45-6789" {
				t.Errorf("Load: tin not opened: %+v", state)
			}
			return err
		})
	})

	t.Run("concurrent Loads on a full pool", func(t *testing.T) {
		const conns = 3
		c := cold(t, tight(t, conns))
		bounded(t, "concurrent Load", func(ctx context.Context) error {
			var wg sync.WaitGroup
			errs := make(chan error, len(payees)*2)
			for round := 0; round < 2; round++ {
				for _, id := range payees {
					wg.Add(1)
					go func() {
						defer wg.Done()
						state, _, err := c.Load(ctx, "Payee", "default", id)
						if err == nil && state.(*billinggen.Payee).Tin != "123-45-6789" {
							t.Errorf("Load %s: tin not opened", id)
						}
						errs <- err
					}()
				}
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

	t.Run("Dispatch reads an existing key in its transaction", func(t *testing.T) {
		c := cold(t, tight(t, 1))
		bounded(t, "Dispatch to a sealed stream", func(ctx context.Context) error {
			return c.Dispatch(ctx, &billinggen.RegisterPayee{
				CommandBase: loom.CommandBase{AggregateID: payees[1], Namespace: "default"},
				Name:        "Payee", Tin: "123-45-6789", TinLast4: "6789",
			})
		})
	})

	t.Run("Dispatch mints a new key in its transaction", func(t *testing.T) {
		p := tight(t, 1)
		c := cold(t, p)
		fresh := uuid.New()
		bounded(t, "Dispatch to a new sealed stream", func(ctx context.Context) error {
			return c.Dispatch(ctx, &billinggen.RegisterPayee{
				CommandBase: loom.CommandBase{AggregateID: fresh, Namespace: "default"},
				Name:        "Fresh", Tin: "987-65-4321", TinLast4: "4321",
			})
		})
		// the minted key committed with the events it sealed, and a second
		// cold client opens them with it
		bounded(t, "Load of the new stream", func(ctx context.Context) error {
			state, _, err := cold(t, p).Load(ctx, "Payee", "default", fresh)
			if err == nil && state.(*billinggen.Payee).Tin != "987-65-4321" {
				t.Errorf("Load after mint: %+v", state)
			}
			return err
		})
		var keys int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_keys WHERE stream_id=$1`, fresh).Scan(&keys); err != nil || keys != 1 {
			t.Fatalf("keys for the new stream: %d (%v)", keys, err)
		}
	})

	t.Run("a runner catches up through readLog on a full pool", func(t *testing.T) {
		// A started client parks two connections (LISTEN and the process
		// lease), so three leaves the runners exactly one. A lagging
		// projection reads the log directly (readLog) and opens each
		// PayeeRegistered's TIN; holding that one connection while the key
		// read waited for another stalled the projection for good.
		rctx, rcancel := context.WithCancel(ctx)
		defer rcancel()
		c := cold(t, tight(t, 3))
		if err := c.Start(rctx, 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			var n int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM loom_entities
				WHERE entity_type='PayeeDirectory' AND data->>'tin' LIKE 'pii:v1:%'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n == len(payees)+1 { // the six seeded and the one minted above
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("payeeDirectory projected %d of %d payees on a full pool", n, len(payees)+1)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})

	t.Run("Entities opens many sealed rows on one connection", func(t *testing.T) {
		c := cold(t, tight(t, 1))
		bounded(t, "Entities", func(ctx context.Context) error {
			got, err := c.Entities(ctx, "PayeeDirectory", "default", payees)
			if err != nil {
				return err
			}
			if len(got) != len(payees) {
				t.Errorf("Entities: %d of %d rows", len(got), len(payees))
			}
			for id, e := range got {
				if e.(*billinggen.PayeeDirectory).Tin != "123-45-6789" {
					t.Errorf("Entities %s: tin not opened", id)
				}
			}
			return nil
		})
	})
}
