package e2e_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
)

// newProcess builds a process nobody has ever deployed before: it
// subscribes to InvoicePaid, performs one journaled effect, and counts
// what it saw. from is its declared start position ("" = @from(head),
// the default).
func newProcess(name, from string, reacted *atomic.Int64, called *atomic.Int64) *loom.ReactorDef {
	return &loom.ReactorDef{
		Name:    name,
		Events:  []string{"InvoicePaid"},
		Subs:    []loom.SubscriptionDef{{Event: "InvoicePaid"}},
		Effects: []string{"notify"},
		From:    from,
		React: func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
			reacted.Add(1)
			_, err := loom.Once(ctx, "notify", func(ctx context.Context) (string, error) {
				called.Add(1)
				return "notified:" + evt.AggregateID.String(), nil
			})
			return nil, err
		},
	}
}

// seedHistory raises and pays invoices with a client that never Starts:
// the log fills up while no runner exists, which is the state a service
// is in the moment before a new process is deployed into it.
func seedHistory(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int) {
	t.Helper()
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		payInvoice(t, ctx, cli, uuid.New())
	}
}

// TestNewProcessStartsAtHead is the HANDOFF ProfileMembersChanged
// incident as a test: a process deployed into a service that already
// has a history must not perform its effect for everything that ever
// happened. With no @from it starts at the log's head — zero reactions
// for the events that predate it, and the very next event reacts
// normally.
func TestNewProcessStartsAtHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	const history = 4
	seedHistory(t, ctx, pool, history)

	var reacted, called atomic.Int64
	reg := billing.NewRegistry()
	reg.Processes = append(reg.Processes, newProcess("notifyOnPaid", "", &reacted, &called))

	billing.Gateway.Reset()
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)

	cli, err := loom.New(loom.Config{DB: pool, Registry: reg, Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// Start seeded the checkpoint before the runner could step: the row
	// exists at the head already, not at 0.
	var seq int64
	if err := pool.QueryRow(ctx, `
		SELECT global_seq FROM loom_checkpoints WHERE service='billing' AND runner='process:notifyOnPaid'`).Scan(&seq); err != nil {
		t.Fatalf("a new from-head process must be checkpointed at Start: %v", err)
	}
	var head int64
	if err := pool.QueryRow(ctx, `SELECT max(global_seq) FROM loom_events WHERE service='billing'`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if seq != head {
		t.Fatalf("checkpoint seeded at %d, want the head %d", seq, head)
	}

	// give the runner every chance to misbehave over the history
	time.Sleep(500 * time.Millisecond)
	if n := reacted.Load(); n != 0 {
		t.Fatalf("the new process reacted %d times to events that predate it, want 0", n)
	}
	if n := called.Load(); n != 0 {
		t.Fatalf("the new process performed %d effects for events that predate it, want 0", n)
	}
	var effects int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_effects WHERE scope LIKE 'process:notifyOnPaid/%'`).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 0 {
		t.Fatalf("%d journaled effects for a process that should have done nothing", effects)
	}

	// but it is a live process: the next event is its business
	payInvoice(t, ctx, cli, uuid.New())
	waitFor(t, ctx, "the new process reacting to an event appended after it existed", func() bool {
		return reacted.Load() == 1 && called.Load() == 1
	})
	time.Sleep(200 * time.Millisecond)
	if n := reacted.Load(); n != 1 {
		t.Fatalf("the new process reacted %d times to one new event, want 1", n)
	}
}

// TestProcessFromOriginReplaysHistory is the other half of the default:
// @from(origin) is the backfilling process's opt-in and still sees the
// whole log, and a process that already has a checkpoint row keeps it —
// seeding never overwrites what a redeployed process had.
func TestProcessFromOriginReplaysHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	const history = 4
	seedHistory(t, ctx, pool, history)

	// the event the resumed process already got through: its checkpoint
	// sits on the second InvoicePaid, so only the later ones are left
	var resumeAt int64
	if err := pool.QueryRow(ctx, `
		SELECT global_seq FROM loom_events
		WHERE service='billing' AND type='InvoicePaid' ORDER BY global_seq OFFSET 1 LIMIT 1`).Scan(&resumeAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq, updated_at) VALUES ('billing','process:resumedOnPaid',$1, now())`,
		resumeAt); err != nil {
		t.Fatal(err)
	}

	var backfilled, backfillCalls, resumed, resumedCalls atomic.Int64
	reg := billing.NewRegistry()
	reg.Processes = append(reg.Processes,
		newProcess("backfillOnPaid", loom.FromOrigin, &backfilled, &backfillCalls),
		newProcess("resumedOnPaid", "", &resumed, &resumedCalls))

	billing.Gateway.Reset()
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)

	cli, err := loom.New(loom.Config{DB: pool, Registry: reg, Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	waitFor(t, ctx, "the @from(origin) process replaying the whole log", func() bool {
		return backfilled.Load() == history
	})
	waitFor(t, ctx, "the resumed process reacting to the events after its checkpoint", func() bool {
		return resumed.Load() == history-2
	})
	time.Sleep(300 * time.Millisecond)
	if n := backfilled.Load(); n != history {
		t.Fatalf("@from(origin) reacted %d times, want the whole history %d", n, history)
	}
	if n := backfillCalls.Load(); n != history {
		t.Fatalf("@from(origin) performed %d effects, want %d", n, history)
	}
	if n := resumed.Load(); n != history-2 {
		t.Fatalf("a process with a checkpoint row reacted %d times, want %d — Start must not move an existing checkpoint", n, history-2)
	}
	var seq int64
	if err := pool.QueryRow(ctx, `
		SELECT global_seq FROM loom_checkpoints WHERE service='billing' AND runner='process:backfillOnPaid'`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("the @from(origin) process never checkpointed")
	}
	var parked int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters`).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	if parked != 0 {
		t.Fatalf("%d events parked", parked)
	}
}
