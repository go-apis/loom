package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
)

// hookedBilling starts a billing client on pool with captureOnPaid's
// reaction replaced by react (nil keeps the generated one), under policy,
// and cfg's hooks.
func hookedBilling(t *testing.T, ctx context.Context, pool *pgxpool.Pool, policy *loom.RetryPolicy,
	react func(context.Context, *loom.Event) ([]loom.Command, error), cfg loom.Config,
) *loom.Client {
	t.Helper()
	reg := billing.NewRegistry()
	for _, p := range reg.Processes {
		if p.Name == "captureOnPaid" {
			p.Retry = policy
			if react != nil {
				p.React = react
			}
		}
	}
	cfg.DB, cfg.Registry, cfg.Keys = pool, reg, testKeys(t)
	cli, err := loom.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return cli
}

// letterLog records OnDeadLetter calls.
type letterLog struct {
	mu      sync.Mutex
	letters []loom.DeadLetter
}

func (l *letterLog) hook(_ context.Context, d loom.DeadLetter) {
	l.mu.Lock()
	l.letters = append(l.letters, d)
	l.mu.Unlock()
}

func (l *letterLog) all() []loom.DeadLetter {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]loom.DeadLetter(nil), l.letters...)
}

// TestPersistentErrorDeadLettersAtOnce: under @retry, a transient failure
// is retried (immediately, then durably) until it passes, while a reaction
// failing with loom.Persistent is dead-lettered on its first attempt, with
// no retry row armed, and OnDeadLetter is called for it once.
func TestPersistentErrorDeadLettersAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	transient, persistent := uuid.New(), uuid.New()
	var mu sync.Mutex
	calls := map[uuid.UUID]int{}
	react := func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
		mu.Lock()
		calls[evt.AggregateID]++
		n := calls[evt.AggregateID]
		mu.Unlock()
		switch {
		case evt.AggregateID == persistent:
			return nil, loom.Persistent(errors.New("provider refused: account closed"))
		case n <= 4: // three immediate attempts, then the first durable one
			return nil, errors.New("provider timed out")
		}
		return nil, nil
	}
	var letters letterLog
	cli := hookedBilling(t, ctx, pool, &loom.RetryPolicy{Max: 5, Min: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond},
		react, loom.Config{OnDeadLetter: letters.hook})
	callsOf := func(id uuid.UUID) int { mu.Lock(); defer mu.Unlock(); return calls[id] }

	payInvoice(t, ctx, cli, transient)
	payInvoice(t, ctx, cli, persistent)

	waitFor(t, ctx, "transient failure to pass on retry", func() bool { return callsOf(transient) == 5 })
	waitFor(t, ctx, "persistent failure to be reported", func() bool { return len(letters.all()) == 1 })
	time.Sleep(500 * time.Millisecond) // any retry would have fired by now

	if n := callsOf(persistent); n != 1 {
		t.Fatalf("persistent failure reacted %d times, want 1 (no retries)", n)
	}
	if n := callsOf(transient); n != 5 {
		t.Fatalf("transient failure reacted %d times, want 5", n)
	}
	got, err := cli.DeadLetters(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Attempts != 1 || got[0].Runner != "process:captureOnPaid" {
		t.Fatalf("dead letters: %+v", got)
	}
	if n := retryRows(t, ctx, pool); n != 0 {
		t.Fatalf("%d retry rows armed", n)
	}
	if l := letters.all(); len(l) != 1 || l[0].ID != got[0].ID {
		t.Fatalf("OnDeadLetter calls: %+v", l)
	}
}

// TestRedriveLadderThenOnDeadLetter: a dead letter that keeps failing is
// redriven once per ladder step, then reported once; one whose failure
// passes heals on the ladder and is never reported.
func TestRedriveLadderThenOnDeadLetter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	broken, passing := uuid.New(), uuid.New()
	var mu sync.Mutex
	calls := map[uuid.UUID]int{}
	react := func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
		mu.Lock()
		calls[evt.AggregateID]++
		n := calls[evt.AggregateID]
		mu.Unlock()
		// the passing one fails its three immediate attempts, then works
		if evt.AggregateID == broken || n <= 3 {
			return nil, fmt.Errorf("downstream unavailable (%d)", n)
		}
		return nil, nil
	}
	var letters letterLog
	cli := hookedBilling(t, ctx, pool, nil, react, loom.Config{
		OnDeadLetter:  letters.hook,
		RedriveLadder: []time.Duration{200 * time.Millisecond, 300 * time.Millisecond},
	})
	callsOf := func(id uuid.UUID) int { mu.Lock(); defer mu.Unlock(); return calls[id] }

	payInvoice(t, ctx, cli, broken)
	payInvoice(t, ctx, cli, passing)

	waitFor(t, ctx, "the broken letter to be reported", func() bool { return len(letters.all()) == 1 })
	time.Sleep(800 * time.Millisecond) // past where a third step would be

	if n := callsOf(broken); n != 3+2 {
		t.Fatalf("broken reaction ran %d times, want 3 immediate + 2 redrives", n)
	}
	if n := callsOf(passing); n != 3+1 {
		t.Fatalf("passing reaction ran %d times, want 3 immediate + 1 redrive", n)
	}
	l := letters.all()
	if len(l) != 1 || l[0].Redrives != 2 || l[0].Error != "downstream unavailable (5)" {
		t.Fatalf("OnDeadLetter calls: %+v", l)
	}
	got, err := cli.DeadLetters(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != l[0].ID {
		t.Fatalf("dead letters left: %+v (the healed one must be gone)", got)
	}
}

// TestOnRunnerLag: a process runner held behind the threshold is reported
// once, by the lease holder of two instances; after it catches up, falling
// behind again is reported again.
func TestOnRunnerLag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	var hold atomic.Bool
	react := func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
		for hold.Load() && ctx.Err() == nil {
			time.Sleep(20 * time.Millisecond)
		}
		return nil, nil
	}
	type call struct {
		lag     loom.RunnerLag
		client  int
		leading bool
	}
	var mu sync.Mutex
	var calls []call
	// the follower would report first (its threshold is shorter) if
	// followers swept at all
	thresholds := []time.Duration{600 * time.Millisecond, 100 * time.Millisecond}
	clients := make([]*loom.Client, 2)
	for i := range clients {
		cfg := loom.Config{RunnerLagThreshold: thresholds[i]}
		cfg.OnRunnerLag = func(_ context.Context, lag loom.RunnerLag) {
			mu.Lock()
			calls = append(calls, call{lag, i, clients[i].Leading()})
			mu.Unlock()
		}
		clients[i] = hookedBilling(t, ctx, pool, nil, react, cfg)
		if i == 0 {
			waitFor(t, ctx, "the first instance to lead", clients[0].Leading)
		}
	}
	callsN := func() int { mu.Lock(); defer mu.Unlock(); return len(calls) }
	notified := func() bool {
		var on bool
		if err := pool.QueryRow(ctx, `
			SELECT lag_notified_at IS NOT NULL FROM loom_checkpoints
			WHERE service='billing' AND runner='process:captureOnPaid'`).Scan(&on); err != nil {
			t.Fatal(err)
		}
		return on
	}

	hold.Store(true)
	payInvoice(t, ctx, clients[0], uuid.New())
	waitFor(t, ctx, "lag reported", func() bool { return callsN() == 1 })
	time.Sleep(900 * time.Millisecond) // still behind: no second call
	if n := callsN(); n != 1 {
		t.Fatalf("OnRunnerLag called %d times while behind, want 1", n)
	}
	mu.Lock()
	first := calls[0]
	mu.Unlock()
	if first.client != 0 || !first.leading || first.lag.Runner != "process:captureOnPaid" || first.lag.Behind <= 600*time.Millisecond || first.lag.Seq <= first.lag.Checkpoint {
		t.Fatalf("first call: %+v", first)
	}

	hold.Store(false)
	waitFor(t, ctx, "runner to catch up", func() bool { return !notified() })
	if n := callsN(); n != 1 {
		t.Fatalf("OnRunnerLag called %d times on recovery", n)
	}

	hold.Store(true)
	payInvoice(t, ctx, clients[0], uuid.New())
	waitFor(t, ctx, "second lag reported", func() bool { return callsN() == 2 })
	hold.Store(false)
	mu.Lock()
	defer mu.Unlock()
	if calls[1].client != 0 || !calls[1].leading {
		t.Fatalf("second call: %+v", calls[1])
	}
}

// TestOnEffectInDoubt: an effect left started by a crashed attempt, which
// nothing can settle, is reported once on the lease holder, however many
// times reactions (retries, a redrive) find it in doubt.
func TestOnEffectInDoubt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	var mu sync.Mutex
	var seen []loom.EffectRecord
	var leading []bool
	var cli *loom.Client
	cfg := loom.Config{OnEffectInDoubt: func(_ context.Context, e loom.EffectRecord) {
		mu.Lock()
		seen = append(seen, e)
		leading = append(leading, cli.Leading())
		mu.Unlock()
	}}
	cfg.DB, cfg.Registry, cfg.Keys = pool, billing.NewRegistry(), testKeys(t)
	var err error
	if cli, err = loom.New(cfg); err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	billing.Gateway.Reset()
	billing.SetFailReactAfterCapture(0)
	payInvoice(t, ctx, cli, uuid.New())
	var seq int64
	if err := pool.QueryRow(ctx, `SELECT global_seq FROM loom_events WHERE service='billing' AND type='InvoicePaid'`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	// what a crash mid-call leaves: the claim, and the checkpoint short of it
	scope := fmt.Sprintf("process:captureOnPaid/billing:%d", seq)
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_effects (service, scope, key, status) VALUES ('billing', $1, 'gateway_capture', 'running')`, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq, updated_at)
		VALUES ('billing', 'process:captureOnPaid', $1, now())`, seq-1); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	seenN := func() int { mu.Lock(); defer mu.Unlock(); return len(seen) }

	waitFor(t, ctx, "in-doubt effect reported", func() bool { return seenN() == 1 })
	waitFor(t, ctx, "reaction to park", func() bool { return deadLetterCount(t, ctx, pool) == 1 })
	letters, err := cli.DeadLetters(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	// finding it in doubt again is the same occurrence
	if err := cli.RedriveDeadLetter(ctx, letters[0].ID); err == nil {
		t.Fatal("redrive of an in-doubt effect succeeded")
	}
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0].Scope != scope || seen[0].Key != "gateway_capture" || seen[0].Status != "running" || !leading[0] {
		t.Fatalf("OnEffectInDoubt calls: %+v (leading %v)", seen, leading)
	}
	if billing.Gateway.CallsN() != 0 {
		t.Fatalf("gateway called %d times on an in-doubt effect", billing.Gateway.CallsN())
	}
}
