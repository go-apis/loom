package e2e_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// paidWatcher is a process that subscribes to InvoicePaid alone and
// keeps the sequence of each one it is handed. block, when set, is
// called inside the reaction, before it counts as handled.
type paidWatcher struct {
	mu      sync.Mutex
	started []int64
	handled []int64
	block   func(ctx context.Context, seq int64) error
}

func (w *paidWatcher) def() *loom.ReactorDef {
	return &loom.ReactorDef{
		Name:   "watchPaid",
		Events: []string{"InvoicePaid"},
		Subs:   []loom.SubscriptionDef{{Event: "InvoicePaid"}},
		From:   loom.FromOrigin,
		React: func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
			w.mu.Lock()
			w.started = append(w.started, evt.GlobalSeq)
			w.mu.Unlock()
			if w.block != nil {
				if err := w.block(ctx, evt.GlobalSeq); err != nil {
					return nil, err
				}
			}
			w.mu.Lock()
			w.handled = append(w.handled, evt.GlobalSeq)
			w.mu.Unlock()
			return nil, nil
		},
	}
}

func (w *paidWatcher) seen() (started, handled []int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.started), slices.Clone(w.handled)
}

// watcherRegistry is billing with watchPaid as its only process: the
// projections stay (invoiceSummary folds the invoices, payeeDirectory
// subscribes to none of them).
func watcherRegistry(w *paidWatcher) *loom.Registry {
	reg := billing.NewRegistry()
	reg.Processes = []*loom.ReactorDef{w.def()}
	return reg
}

// countCheckpointWrites records every write that sets a checkpoint's
// position, by a trigger: the count is what Postgres did, not what the
// runner says it did.
func countCheckpointWrites(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, sql := range []string{
		`CREATE TABLE checkpoint_writes (runner text NOT NULL, global_seq bigint NOT NULL)`,
		`CREATE FUNCTION note_checkpoint_write() RETURNS trigger AS $$
		 BEGIN
			INSERT INTO checkpoint_writes VALUES (NEW.runner, NEW.global_seq);
			RETURN NEW;
		 END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER checkpoint_written AFTER INSERT OR UPDATE OF global_seq ON loom_checkpoints
		 FOR EACH ROW EXECUTE FUNCTION note_checkpoint_write()`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
}

func checkpointWrites(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runner string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM checkpoint_writes WHERE runner=$1`, runner).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// raiseInvoices appends n InvoiceRaised events, each the first event of
// an invoice of its own, as copies of one raised through a dispatch: ten
// thousand dispatches would be the slow part of the test, and nothing is
// running yet to care how they got there.
func raiseInvoices(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cli *loom.Client, n int) {
	t.Helper()
	first := uuid.New()
	if err := cli.Dispatch(ctx, &billinggen.RaiseInvoice{
		CommandBase: loom.CommandBase{AggregateID: first, Namespace: "default"},
		CustomerId:  uuid.New(), AmountCents: 4200, Currency: "USD",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, schema_version, at, data)
		SELECT e.service, e.namespace, e.aggregate_type, gen_random_uuid(), 1, e.type, e.schema_version, now(), e.data
		FROM loom_events e, generate_series(1, $2)
		WHERE e.service='billing' AND e.aggregate_id=$1 AND e.type='InvoiceRaised'`,
		first, n-1); err != nil {
		t.Fatal(err)
	}
}

func runnerCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runner string) int64 {
	t.Helper()
	var seq int64
	_ = pool.QueryRow(ctx, `SELECT global_seq FROM loom_checkpoints WHERE service='billing' AND runner=$1`, runner).Scan(&seq)
	return seq
}

// lastPaid is the sequence of the newest InvoicePaid: paying an invoice
// also posts its ledger entry (the postLedger policy), so it is not the
// log's head.
func lastPaid(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var seq int64
	if err := pool.QueryRow(ctx, `SELECT max(global_seq) FROM loom_events WHERE service='billing' AND type='InvoicePaid'`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

func logHead(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var head int64
	if err := pool.QueryRow(ctx, `SELECT max(global_seq) FROM loom_events WHERE service='billing'`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	return head
}

// TestAProcessPassesIgnoredEventsInOneWrite is runsheet's 18 November as
// a test: one loop wrote 84,000 events of one type, and every other
// process walked them one checkpoint write at a time, for hours. A run
// of events a process does not subscribe to now costs it a write or two
// however long it is, and what it does subscribe to is still delivered
// in order, once, across a crash.
func TestAProcessPassesIgnoredEventsInOneWrite(t *testing.T) {
	t.Run("ten thousand ignored events, then one of its own", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		pool := testDB(t, ctx)

		w := &paidWatcher{}
		cli, err := loom.New(loom.Config{DB: pool, Registry: watcherRegistry(w), Keys: testKeys(t)})
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		const ignored = 10_000
		raiseInvoices(t, ctx, pool, cli, ignored)
		payInvoice(t, ctx, cli, uuid.New()) // one more raised, then the paid it waits for
		head, paid := logHead(t, ctx, pool), lastPaid(t, ctx, pool)
		if head < ignored+2 {
			t.Fatalf("the log holds %d events, want at least %d", head, ignored+2)
		}

		countCheckpointWrites(t, ctx, pool)
		if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		waitFor(t, ctx, "the process handling its one event", func() bool {
			_, handled := w.seen()
			return len(handled) == 1
		})
		if _, handled := w.seen(); handled[0] != paid {
			t.Fatalf("handled seq %d, want the InvoicePaid at %d", handled[0], paid)
		}
		waitFor(t, ctx, "the process checkpoint at the head", func() bool {
			return runnerCheckpoint(t, ctx, pool, "process:watchPaid") == head
		})
		if n := checkpointWrites(t, ctx, pool, "process:watchPaid"); n > 20 {
			t.Fatalf("the process wrote its checkpoint %d times to pass %d ignored events and handle one, want at most 20", n, ignored+1)
		} else {
			t.Logf("process: %d checkpoint writes for %d events", n, head)
		}

		// a projection that folds none of them passes the run the same way
		waitFor(t, ctx, "payeeDirectory at the head", func() bool {
			return runnerCheckpoint(t, ctx, pool, "projection:payeeDirectory") == head
		})
		if n := checkpointWrites(t, ctx, pool, "projection:payeeDirectory"); n > 20 {
			t.Fatalf("a projection that folds none of %d events wrote its checkpoint %d times, want at most 20", head, n)
		}
		// and one that folds them all still folded them all
		// (ten thousand folds: longer than waitFor's fifteen seconds on a
		// busy machine)
		for runnerCheckpoint(t, ctx, pool, "projection:invoiceSummary") != head {
			if ctx.Err() != nil {
				t.Fatalf("invoiceSummary stopped at %d, short of the head %d", runnerCheckpoint(t, ctx, pool, "projection:invoiceSummary"), head)
			}
			time.Sleep(100 * time.Millisecond)
		}
		var rows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_entities WHERE service='billing' AND entity_type='InvoiceSummary'`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != ignored+1 {
			t.Fatalf("invoiceSummary holds %d rows, want %d: it subscribes to every one of these events", rows, ignored+1)
		}

		// still a live process: the next event of its own is delivered
		payInvoice(t, ctx, cli, uuid.New())
		waitFor(t, ctx, "the next InvoicePaid", func() bool {
			_, handled := w.seen()
			return len(handled) == 2
		})
		time.Sleep(200 * time.Millisecond)
		if _, handled := w.seen(); len(handled) != 2 {
			t.Fatalf("handled %v, want two events once each", handled)
		}
	})

	t.Run("a crash mid-run redelivers nothing already handled", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		pool := testDB(t, ctx)

		seed, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
		if err != nil {
			t.Fatal(err)
		}
		if err := seed.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		// paid, 700 ignored, paid, 700 ignored, paid: each run longer than
		// one batch read, so each is passed by looking ahead
		var paid []int64
		for i := 0; i < 3; i++ {
			if i > 0 {
				raiseInvoices(t, ctx, pool, seed, 700)
			}
			payInvoice(t, ctx, seed, uuid.New())
			paid = append(paid, lastPaid(t, ctx, pool))
		}

		// the first instance dies inside its reaction to the second paid
		first := &paidWatcher{}
		reached := make(chan struct{})
		first.block = func(ctx context.Context, seq int64) error {
			if seq != paid[1] {
				return nil
			}
			close(reached)
			<-ctx.Done()
			return ctx.Err()
		}
		a, err := loom.New(loom.Config{DB: pool, Registry: watcherRegistry(first), Keys: testKeys(t)})
		if err != nil {
			t.Fatal(err)
		}
		actx, crash := context.WithCancel(ctx)
		defer crash()
		if err := a.Start(actx, 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		select {
		case <-reached:
		case <-ctx.Done():
			t.Fatal("the first instance never reached its second event")
		}
		crash()
		waitFor(t, ctx, "the first instance giving up the lease", func() bool { return !a.Leading() })

		started, handled := first.seen()
		if !slices.Equal(handled, paid[:1]) || !slices.Equal(started, paid[:2]) {
			t.Fatalf("before the crash: started %v, handled %v; want started %v, handled %v", started, handled, paid[:2], paid[:1])
		}
		// the checkpoint sits at or past everything handled and short of
		// the event the crash interrupted: the ignored events between
		// are passed in memory and written when due (checkpoint.go), so
		// the row may hold the handled event or any of them
		if seq := runnerCheckpoint(t, ctx, pool, "process:watchPaid"); seq < paid[0] || seq >= paid[1] {
			t.Fatalf("checkpoint at %d after the crash, want from %d (the handled event) to %d (just short of the interrupted event at %d)", seq, paid[0], paid[1]-1, paid[1])
		}
		var parked int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters`).Scan(&parked); err != nil {
			t.Fatal(err)
		}
		if parked != 0 {
			t.Fatalf("%d dead letters: the crash must leave the event to be delivered again, not park it", parked)
		}

		// its replacement takes over from the checkpoint
		second := &paidWatcher{}
		b, err := loom.New(loom.Config{DB: pool, Registry: watcherRegistry(second), Keys: testKeys(t)})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Start(ctx, 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		waitFor(t, ctx, "the second instance handling what was left", func() bool {
			_, handled := second.seen()
			return len(handled) >= 2
		})
		head := logHead(t, ctx, pool)
		waitFor(t, ctx, "the checkpoint at the head", func() bool {
			return runnerCheckpoint(t, ctx, pool, "process:watchPaid") == head
		})
		time.Sleep(200 * time.Millisecond)
		if started, handled := second.seen(); !slices.Equal(handled, paid[1:]) || !slices.Equal(started, paid[1:]) {
			t.Fatalf("after the crash the second instance started %v and handled %v, want exactly %v: the interrupted event and the one after, not the one already handled", started, handled, paid[1:])
		}
	})
}
