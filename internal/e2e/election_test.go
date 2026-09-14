package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
)

// TestProcessElection runs three instances of one service on one
// database — Cloud Run scaled out — and pays invoices. The capture
// process must react to each event once across the fleet: the gateway
// called once per invoice, no reaction parked, no effect in doubt.
// Projections have elected a worker per runner by advisory lock since
// the start; processes must too, or every instance reacts and the
// losers of the effect-claim race park. Then the leader goes away and
// another instance carries on.
func TestProcessElection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	const instances = 3
	var clis []*loom.Client
	var cancels []context.CancelFunc
	for i := 0; i < instances; i++ {
		cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if err := cli.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
		}
		ictx, icancel := context.WithCancel(ctx)
		if err := cli.Start(ictx, 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		clis = append(clis, cli)
		cancels = append(cancels, icancel)
	}
	waitFor(t, ctx, "one leader", func() bool {
		n := 0
		for _, cli := range clis {
			if cli.Leading() {
				n++
			}
		}
		return n == 1
	})

	billing.Gateway.Reset()
	billing.Gateway.SetDelay(150 * time.Millisecond) // a call long enough to overlap
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)
	billing.ResetReactions()

	const invoices = 12
	for i := 0; i < invoices; i++ {
		payInvoice(t, ctx, clis[i%instances], uuid.New())
	}
	waitFor(t, ctx, "every instance's process checkpoint at the head", func() bool {
		var behind int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM loom_checkpoints c
			WHERE c.service='billing' AND c.runner='process:captureOnPaid'
			  AND c.global_seq < (SELECT max(global_seq) FROM loom_events WHERE service='billing')`).Scan(&behind)
		var exists int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM loom_checkpoints WHERE service='billing' AND runner='process:captureOnPaid'`).Scan(&exists)
		return err == nil && behind == 0 && exists == 1
	})
	// let any straggler instance take its turn at the (already advanced)
	// checkpoint before counting
	time.Sleep(300 * time.Millisecond)

	if n := billing.Gateway.CallsN(); n != invoices {
		t.Fatalf("gateway called %d times across %d instances, want %d", n, instances, invoices)
	}
	if n := billing.Reactions(); n != invoices {
		t.Fatalf("the process reacted %d times across %d instances, want %d — one instance at a time", n, instances, invoices)
	}
	var parked, doubt int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters`).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_effects WHERE status <> 'done'`).Scan(&doubt); err != nil {
		t.Fatal(err)
	}
	if parked != 0 || doubt != 0 {
		t.Fatalf("with %d instances: %d parked, %d effects not done — the process runner is not elected", instances, parked, doubt)
	}
	leaders := 0
	for _, cli := range clis {
		if cli.Leading() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders, want exactly one", leaders)
	}

	// the leader is scaled down: its lease returns with its context, a
	// follower takes it at the next poll, and the fleet keeps reacting
	var gone int
	for i, cli := range clis {
		if cli.Leading() {
			cancels[i]()
			gone = i
		}
	}
	waitFor(t, ctx, "a new leader", func() bool {
		for i, cli := range clis {
			if i != gone && cli.Leading() {
				return true
			}
		}
		return false
	})
	billing.ResetReactions()
	billing.Gateway.Reset()
	billing.Gateway.SetDelay(20 * time.Millisecond)
	const more = 6
	for i := 0; i < more; i++ {
		next := (gone + 1 + i) % instances
		payInvoice(t, ctx, clis[next], uuid.New())
	}
	waitFor(t, ctx, "captures after failover", func() bool { return billing.Gateway.CallsN() == more })
	time.Sleep(200 * time.Millisecond)
	if n := billing.Reactions(); n != more {
		t.Fatalf("after failover the process reacted %d times, want %d", n, more)
	}
	if clis[gone].Leading() {
		t.Fatal("a stopped instance must not believe it leads")
	}
}
