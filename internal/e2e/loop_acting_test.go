package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// TestALoopThatKeepsActingWithoutProgressIsBackedOff is runsheet's item
// loop of 18 November as a test: a look that sends a command every time
// it is called, where the command's own event calls the look again, ran
// flat out — a deploy polled every 1.4s on 18 items for three hours.
// Here a kiln's look stokes it on every look and nothing loom records
// about the kiln changes. Through a simulated hour (the store's row is
// moved back to each next look as it is written, so the runner makes it
// at once) the looks come 1s, 2s, 4s … apart up to the minute: 65 in the
// hour, six of them sooner than the minute, where flat out is 3,600.
// The service is told once. An event from outside is still looked at
// within the second, ends the spell, and a new spell is told again.
func TestALoopThatKeepsActingWithoutProgressIsBackedOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	var (
		mu   sync.Mutex
		told []loom.LoopActing
	)
	hooked := func() []loom.LoopActing {
		mu.Lock()
		defer mu.Unlock()
		return append([]loom.LoopActing(nil), told...)
	}
	cli, pool := loopClockWith(t, ctx, true, loom.Config{
		OnLoopActing: func(_ context.Context, acting loom.LoopActing) {
			mu.Lock()
			defer mu.Unlock()
			told = append(told, acting)
		},
	})
	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		return []loom.Command{&clockgen.StokeKiln{CommandBase: kiln}}, nil
	})
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}

	// settled waits for look n to have ended: made, and its row written
	settled := func(n int) loom.LoopDue {
		t.Helper()
		waitFor(t, ctx, "the look to end", func() bool {
			if made, _ := clock.KilnLooks(kiln.AggregateID.String()); made < n {
				return false
			}
			var free bool
			err := pool.QueryRow(ctx, `
				SELECT leased_until IS NULL AND looked_at IS NOT NULL FROM loom_loop_due
				WHERE service='clock' AND aggregate_id=$1`, kiln.AggregateID).Scan(&free)
			return err == nil && free
		})
		return dueOf(t, ctx, cli, kiln)
	}
	// bring moves the kiln's row back in time by however long it has
	// left to its next look, so the look is due now: the hour is walked
	// without waiting for it. A row a runner holds is left alone.
	bring := func() {
		t.Helper()
		var by float64
		err := pool.QueryRow(ctx, `
			UPDATE loom_loop_due d SET
				looked_at = looked_at - w.by, event_at = event_at - w.by,
				outside_at = outside_at - w.by, next_look = next_look - w.by
			FROM (SELECT next_look - clock_timestamp() AS by FROM loom_loop_due
			      WHERE service='clock' AND aggregate_id=$1) w
			WHERE d.service='clock' AND d.aggregate_id=$1 AND d.leased_until IS NULL AND w.by > interval '0'
			RETURNING extract(epoch FROM w.by)::float8`, kiln.AggregateID).Scan(&by)
		if err != nil && err != pgx.ErrNoRows {
			t.Fatal(err)
		}
	}

	var (
		gaps      []time.Duration
		simulated time.Duration
		looks     int
	)
	for {
		looks++
		due := settled(looks)
		if due.Acting != looks {
			t.Fatalf("after look %d the kiln has acted without progress %d times in a row, want %d", looks, due.Acting, looks)
		}
		gap := due.NextLook.Sub(due.Looked)
		if simulated+gap > time.Hour {
			break
		}
		simulated += gap
		gaps = append(gaps, gap)
		bring()
	}
	t.Logf("%d looks in a simulated hour; the first apart %v", looks, gaps[:min(8, len(gaps))])

	if looks >= 70 {
		t.Fatalf("%d looks in a simulated hour, want 65: backed off to one a minute", looks)
	}
	soon := 0
	for _, g := range gaps {
		if g < loom.LoopEvery {
			soon++
		}
	}
	if soon >= 20 {
		t.Fatalf("%d looks in the hour came sooner than the minute after the one before, want fewer than 20", soon)
	}
	// the first follows its own event within the second, as ever; from
	// the second acting look they double, to the minute
	if gaps[0] < loom.LoopSoon || gaps[0] > 2*loom.LoopSoon {
		t.Fatalf("the second look is %s after the first, want about %s", gaps[0], loom.LoopSoon)
	}
	for i, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
		if gaps[i+1] != want {
			t.Fatalf("the looks are %v apart, want 2s doubling to the minute after the first", gaps[:8])
		}
	}

	// the service was told, once, of this kiln
	calls := hooked()
	if len(calls) != 1 {
		t.Fatalf("OnLoopActing was called %d times in the hour, want once", len(calls))
	}
	if c := calls[0]; c.Service != "clock" || c.Aggregate != "Kiln" || c.Namespace != kiln.Namespace || c.ID != kiln.AggregateID ||
		c.Looks != loom.LoopActingFrom || len(c.Commands) != 1 || c.Commands[0] != "StokeKiln" || c.NextLook.Sub(c.At) != 2*time.Second {
		t.Fatalf("OnLoopActing was told %+v", c)
	}

	// an event nobody's look caused is not held back: the next look is
	// within the second, a minute's backoff or not
	before := dueOf(t, ctx, cli, kiln)
	if wait := time.Until(before.NextLook); wait < 30*time.Second {
		t.Fatalf("before the outside event the next look is %s away, want it backed off to about the minute", wait)
	}
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	if wait := time.Until(dueOf(t, ctx, cli, kiln).NextLook); wait > loom.LoopSoon {
		t.Fatalf("after an outside event the next look is %s away, want within %s", wait, loom.LoopSoon)
	}
	// it ends the spell: the look it brings starts a run of its own, and
	// when that run is a spell too the service is told of it, once more
	waitFor(t, ctx, "the look for the outside event", func() bool {
		made, _ := clock.KilnLooks(kiln.AggregateID.String())
		return made > looks
	})
	waitFor(t, ctx, "the service being told of the new spell", func() bool { return len(hooked()) == 2 })
	if due := settled(looks + 2); due.Acting < loom.LoopActingFrom || due.Acting > 3 {
		t.Fatalf("after the outside event the run stands at %d, want it started over", due.Acting)
	}
	time.Sleep(500 * time.Millisecond)
	if n := len(hooked()); n != 2 {
		t.Fatalf("OnLoopActing was called %d times over two spells, want twice", n)
	}
}
