package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
)

// TestReplayProcess: a replay hands a process events it has been past,
// and its effects run again — the journal that makes the process's own
// retries call the gateway once does not answer for a replay. It can be
// counted before it is run, narrowed to one stream, and it leaves the
// process's checkpoint where it was.
func TestReplayProcess(t *testing.T) {
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
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	billing.Gateway.Reset()
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)

	// three invoices paid: the process captures each once
	invoices := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, id := range invoices {
		payInvoice(t, ctx, cli, id)
	}
	waitFor(t, ctx, "the process to capture all three", func() bool { return billing.Gateway.CallsN() == 3 })
	checkpoint := func() int64 {
		t.Helper()
		st, err := cli.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range st.Runners {
			if r.Runner == "process:captureOnPaid" {
				return r.Seq
			}
		}
		t.Fatal("no runner process:captureOnPaid")
		return 0
	}
	waitFor(t, ctx, "the process to be caught up", func() bool {
		st, err := cli.Status(ctx)
		return err == nil && st.Behind == 0
	})
	before := checkpoint()

	// counted first: nothing is called
	dry, err := cli.ReplayProcess(ctx, "captureOnPaid", loom.ReplayOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if dry.Events != 3 || dry.Reacted != 0 || billing.Gateway.CallsN() != 3 {
		t.Fatalf("dry run = %+v, gateway called %d times; want 3 events counted and no call", dry, billing.Gateway.CallsN())
	}

	// one stream: its capture runs again, the others' do not
	one, err := cli.ReplayProcess(ctx, "captureOnPaid", loom.ReplayOptions{Namespace: "default", AggregateID: invoices[1]})
	if err != nil {
		t.Fatal(err)
	}
	if one.Events != 1 || one.Reacted != 1 || one.Parked != 0 || billing.Gateway.CallsN() != 4 || billing.LastReceipt() != "cap_"+invoices[1].String() {
		t.Fatalf("replay of one stream = %+v, gateway called %d times, last receipt %q", one, billing.Gateway.CallsN(), billing.LastReceipt())
	}

	// everything: three more calls, each replay under its own id
	all, err := cli.ReplayProcess(ctx, "captureOnPaid", loom.ReplayOptions{LatestPerAggregate: true})
	if err != nil {
		t.Fatal(err)
	}
	if all.Events != 3 || all.Reacted != 3 || all.ID == one.ID || billing.Gateway.CallsN() != 7 {
		t.Fatalf("replay of all = %+v, gateway called %d times; want 3 reacted and 7 calls in all", all, billing.Gateway.CallsN())
	}

	// a reaction that fails through its retries is parked, and counted
	billing.Gateway.SetFailCalls(100)
	failed, err := cli.ReplayProcess(ctx, "captureOnPaid", loom.ReplayOptions{AggregateID: invoices[0]})
	billing.Gateway.SetFailCalls(0)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Events != 1 || failed.Reacted != 0 || failed.Parked != 1 {
		t.Fatalf("a failing replay = %+v; want it parked", failed)
	}
	dead, err := cli.DeadLetters(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Runner != "process:captureOnPaid (replay)" {
		t.Fatalf("dead letters = %+v; want the replay's one", dead)
	}

	// the process's own position is untouched
	if after := checkpoint(); after != before {
		t.Errorf("the replay moved the process's checkpoint from %d to %d", before, after)
	}

	// what cannot be replayed says so
	for process, opts := range map[string]loom.ReplayOptions{
		"nobody":       {},
		"raiseOnOrder": {}, // reacts only to another service's events
	} {
		if _, err := cli.ReplayProcess(ctx, process, opts); err == nil {
			t.Errorf("replaying %s was accepted", process)
		}
	}
	if _, err := cli.ReplayProcess(ctx, "captureOnPaid", loom.ReplayOptions{Types: []string{"InvoiceRaised"}}); err == nil || !strings.Contains(err.Error(), "does not react to") {
		t.Errorf("replaying a type the process does not subscribe to: %v", err)
	}
}
