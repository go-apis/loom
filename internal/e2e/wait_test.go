package e2e_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

func kilnFired(t *testing.T, ctx context.Context, cli *loom.Client, kiln loom.CommandBase) bool {
	t.Helper()
	state, _, err := cli.Load(ctx, "Kiln", kiln.Namespace, kiln.AggregateID)
	if err != nil {
		t.Fatal(err)
	}
	return state.(*clockgen.Kiln).Fired
}

// TestWaitExpiredCallsItsLoop: a look starts a wait on someone, with a
// deadline. Nothing happens while the wait runs. When the deadline
// passes with no answer, the instance's look is called, told the wait
// has expired and who was to answer, and acts on it. The expiry is told
// once.
func TestWaitExpiredCallsItsLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)

	var expiries atomic.Int64
	var owner atomic.Value
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		if w, late := told.ExpiredWait("smith"); late {
			expiries.Add(1)
			owner.Store(w.Owner)
			// the smith did not come: fire it without them
			return []loom.Command{&clockgen.RecordKilnFired{CommandBase: loom.CommandBase{Namespace: told.Namespace, AggregateID: told.ID}}}, nil
		}
		state, _, err := loom.Load(ctx, "Kiln", told.Namespace, told.ID)
		if err != nil {
			return nil, err
		}
		if !state.(*clockgen.Kiln).Fired && len(told.Waiting) == 0 {
			return nil, loom.WaitFor(ctx, loom.Wait{Name: "smith", Owner: "user:smith", Deadline: told.At.Add(1500 * time.Millisecond)})
		}
		return nil, nil
	})

	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	// the first look starts the wait: it is kept with the instance, and
	// its deadline is the next look
	waitFor(t, ctx, "the look to start its wait", func() bool {
		due, ok, err := cli.LoopDue(ctx, "Kiln", kiln.Namespace, kiln.AggregateID)
		return err == nil && ok && len(due.Waits) == 1
	})
	due := dueOf(t, ctx, cli, kiln)
	if w := due.Waits[0]; w.Name != "smith" || w.Owner != "user:smith" || !due.NextLook.Equal(w.Deadline.Truncate(time.Microsecond)) && due.NextLook.After(w.Deadline) {
		t.Errorf("the wait as kept: %+v, next look %s", due.Waits, due.NextLook)
	}
	// while it runs, nothing
	time.Sleep(600 * time.Millisecond)
	if kilnFired(t, ctx, cli, kiln) || expiries.Load() != 0 {
		t.Fatal("the wait expired before its deadline")
	}
	// the deadline passes: the look is told, and acts
	waitFor(t, ctx, "the kiln to be fired once the wait expired", func() bool { return kilnFired(t, ctx, cli, kiln) })
	if got, _ := owner.Load().(string); got != "user:smith" {
		t.Errorf("the look was told the wait's owner is %q", got)
	}
	// told once: the looks that follow are not told it again
	time.Sleep(1500 * time.Millisecond)
	if n := expiries.Load(); n != 1 {
		t.Errorf("the expiry was told %d times, want once", n)
	}
	if due := dueOf(t, ctx, cli, kiln); len(due.Waits) != 0 {
		t.Errorf("a spent wait is still kept: %+v", due.Waits)
	}
}

// TestWaitAnsweredNeverExpires: a wait that is ended before its
// deadline — by the look, on the event that answers it, or from outside
// — does not call the loop as expired. A wait started from outside a
// look calls it the same way one started inside does. A wait with no
// owner or no deadline is refused, as is one on an aggregate with no
// loop.
func TestWaitAnsweredNeverExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)

	var expired atomic.Value // the names of waits told expired, joined
	expired.Store("")
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		for _, w := range told.Expired {
			expired.Store(expired.Load().(string) + w.Name + " ")
		}
		// an event is the answer the potter was waited on for
		if told.Event {
			loom.StopWaiting(ctx, "potter")
		}
		return nil, nil
	})

	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the first look", func() bool { n, _ := clock.KilnLooks(kiln.AggregateID.String()); return n >= 1 })

	// three waits from outside, a second and a half out
	deadline := time.Now().Add(1500 * time.Millisecond)
	for _, name := range []string{"potter", "glazer", "carter"} {
		if err := cli.Wait(ctx, "Kiln", kiln.Namespace, kiln.AggregateID, loom.Wait{Name: name, Owner: "user:" + name, Deadline: deadline}); err != nil {
			t.Fatal(err)
		}
	}
	if due := dueOf(t, ctx, cli, kiln); len(due.Waits) != 3 || due.NextLook.After(deadline) {
		t.Fatalf("three waits from outside: %+v, next look %s", due.Waits, due.NextLook)
	}
	// the potter answers with an event; the glazer is ended from outside
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	if err := cli.EndWait(ctx, "Kiln", kiln.Namespace, kiln.AggregateID, "glazer"); err != nil {
		t.Fatal(err)
	}
	// only the carter's wait runs out
	waitFor(t, ctx, "the carter's wait to expire", func() bool { return strings.Contains(expired.Load().(string), "carter") })
	time.Sleep(500 * time.Millisecond)
	if got := strings.TrimSpace(expired.Load().(string)); got != "carter" {
		t.Errorf("waits told expired: %q, want carter alone", got)
	}

	// what a wait must say
	for _, bad := range []loom.Wait{
		{Owner: "user:x", Deadline: deadline},
		{Name: "x", Deadline: deadline},
		{Name: "x", Owner: "user:x"},
	} {
		if err := cli.Wait(ctx, "Kiln", kiln.Namespace, kiln.AggregateID, bad); err == nil {
			t.Errorf("a wait of %+v was kept", bad)
		}
	}
	if err := cli.Wait(ctx, "Bell", "tower", uuid.New(), loom.Wait{Name: "x", Owner: "user:x", Deadline: deadline}); err == nil || !strings.Contains(err.Error(), "no @loop") {
		t.Errorf("a wait on an aggregate with no loop: %v", err)
	}
	if err := loom.WaitFor(ctx, loom.Wait{Name: "x", Owner: "user:x", Deadline: deadline}); err == nil || !strings.Contains(err.Error(), "outside a look") {
		t.Errorf("WaitFor outside a look: %v", err)
	}
	// ending a wait that is not there, or for an instance with no looks, is nothing
	if err := cli.EndWait(ctx, "Kiln", kiln.Namespace, uuid.New(), "nobody"); err != nil {
		t.Errorf("ending a wait that is not there: %v", err)
	}
}
