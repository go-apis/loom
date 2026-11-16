package e2e_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// TestForkReplaysARecordedRun: forking a paid invoice re-runs it in
// another namespace on the code deployed now. The reaction to the copied
// payment replays the capture the source recorded, so the gateway is not
// called again. A fork taken before the payment has no capture to
// replay: paying it there is a step past the fork point, and reaches the
// gateway. The source is untouched, and a fork is refused into its own
// namespace, over a stream that exists, and for an @table aggregate.
func TestForkReplaysARecordedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
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

	invoice := uuid.New()
	payInvoice(t, ctx, cli, invoice)
	waitFor(t, ctx, "the source's capture", func() bool { return billing.LastReceipt() == "cap_"+invoice.String() })
	if n := billing.Gateway.CallsN(); n != 1 {
		t.Fatalf("the source captured %d times, want 1", n)
	}

	// the whole run, forked: its reaction replays the recorded capture
	billing.ResetReactions()
	billing.SetLastReceipt("")
	res, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Invoice", Namespace: "default", ID: invoice, ToNamespace: "replay"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Events != 2 || res.Version != 2 || res.Effects != 1 || len(res.Seqs) != 2 {
		t.Fatalf("the fork copied %+v, want 2 events, version 2 and the capture", res)
	}
	waitFor(t, ctx, "the fork's reaction to the copied payment", func() bool { return billing.LastReceipt() == "cap_"+invoice.String() })
	if r := billing.Reactions(); r != 1 {
		t.Errorf("the fork's payment was reacted to %d times, want 1", r)
	}
	if n := billing.Gateway.CallsN(); n != 1 {
		t.Errorf("forking called the gateway again: %d calls, want the source's 1", n)
	}
	state, version, err := cli.Load(ctx, "Invoice", "replay", invoice)
	if err != nil {
		t.Fatal(err)
	}
	if inv := state.(*billinggen.Invoice); version != 2 || inv.PaidAt == nil || inv.AmountCents != 4200 {
		t.Errorf("the fork reads version %d, %+v; want the source's paid invoice", version, inv)
	}
	var cause string
	if err := pool.QueryRow(ctx, `SELECT causation_id FROM loom_events WHERE namespace='replay' AND aggregate_id=$1 AND version=2`, invoice).Scan(&cause); err != nil {
		t.Fatal(err)
	}
	var srcSeq int64
	if err := pool.QueryRow(ctx, `SELECT global_seq FROM loom_events WHERE namespace='default' AND aggregate_id=$1 AND version=2`, invoice).Scan(&srcSeq); err != nil {
		t.Fatal(err)
	}
	if ns, id, seq, ok := loom.ForkedFrom(&loom.Event{Meta: loom.Metadata{CausationID: cause}}); !ok || ns != "default" || id != invoice || seq != srcSeq {
		t.Errorf("the copy's cause %q reads back as %s %s @%d (%v), want default %s @%d", cause, ns, id, seq, ok, invoice, srcSeq)
	}

	// forked at step 1, before the payment: paying it there is new, and
	// reaches the gateway
	early, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Invoice", Namespace: "default", ID: invoice, UpTo: 1, ToNamespace: "rewound"})
	if err != nil {
		t.Fatal(err)
	}
	if early.Events != 1 || early.Version != 1 || early.Effects != 0 {
		t.Fatalf("the rewound fork copied %+v, want the raise alone", early)
	}
	if err := cli.Dispatch(ctx, &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "rewound"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the rewound fork's own capture", func() bool { return billing.Gateway.CallsN() == 2 })

	// the source is as it was
	if _, version, err := cli.Load(ctx, "Invoice", "default", invoice); err != nil || version != 2 {
		t.Errorf("the source reads version %d (%v) after forking, want 2", version, err)
	}

	for name, opts := range map[string]loom.ForkOptions{
		"into its own namespace":     {Aggregate: "Invoice", Namespace: "default", ID: invoice, ToNamespace: "default"},
		"over a stream that exists":  {Aggregate: "Invoice", Namespace: "default", ID: invoice, ToNamespace: "replay"},
		"of an @table aggregate":     {Aggregate: "Payee", Namespace: "default", ID: uuid.New(), ToNamespace: "replay"},
		"of a stream with no events": {Aggregate: "Invoice", Namespace: "default", ID: uuid.New(), ToNamespace: "elsewhere"},
	} {
		if _, err := cli.ForkStream(ctx, opts); err == nil {
			t.Errorf("a fork %s was not refused", name)
		}
	}
}

// TestForkALoopInstanceMidRun: a @loop instance forked part-way is looked
// at in its new namespace from where the fork left it, by the look
// deployed now; the source carries on as it was.
func TestForkALoopInstanceMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)

	// today's look fires a kiln stoked twice
	var fireAt atomic.Int64
	fireAt.Store(2)
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		state, _, err := loom.Load(ctx, "Kiln", told.Namespace, told.ID)
		if err != nil {
			return nil, err
		}
		if kiln := state.(*clockgen.Kiln); !kiln.Fired && kiln.Stokes >= fireAt.Load() {
			return []loom.Command{&clockgen.RecordKilnFired{CommandBase: loom.CommandBase{Namespace: told.Namespace, AggregateID: told.ID}}}, nil
		}
		return nil, nil
	})
	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}

	// fork it after one stoke, with a new look that fires at one
	fireAt.Store(1)
	res, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Kiln", Namespace: "pottery", ID: kiln.AggregateID, UpTo: 2, ToNamespace: "pottery-fork"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 2 {
		t.Fatalf("the fork is at version %d, want 2", res.Version)
	}
	fired := func(ns string) bool {
		state, _, err := cli.Load(ctx, "Kiln", ns, kiln.AggregateID)
		if err != nil {
			t.Fatal(err)
		}
		return state.(*clockgen.Kiln).Fired
	}
	waitFor(t, ctx, "the forked kiln to be fired by today's look", func() bool { return fired("pottery-fork") })
	if _, ok, err := cli.LoopDue(ctx, "Kiln", "pottery-fork", kiln.AggregateID); err != nil || !ok {
		t.Errorf("the forked kiln has no looks due (%v)", err)
	}
	// the source was not touched by the fork; its own look now fires it
	// too, at one stoke, which is the source's own business
	if _, version, err := cli.Load(ctx, "Kiln", "pottery", kiln.AggregateID); err != nil || version < 2 {
		t.Errorf("the source reads version %d (%v)", version, err)
	}
}
