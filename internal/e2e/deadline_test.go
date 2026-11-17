package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// TestALoopKeepsItsDeclaredDeadline: an oven declares `deadline overheat
// after 2s while status in ("heating")`. Heated, it holds the wait;
// rested before 2s, it holds none; heated again, a new one; left heating,
// its look is told once that it expired, and no new wait starts while it
// stays heating; rested and heated again, the deadline starts afresh.
func TestALoopKeepsItsDeclaredDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)
	oven := loom.CommandBase{Namespace: "bakery", AggregateID: uuid.New()}
	wait := loom.DeadlineWait("overheat")

	held := func() (loom.Wait, bool) {
		due, ok, err := cli.LoopDue(ctx, "Oven", oven.Namespace, oven.AggregateID)
		if err != nil || !ok {
			return loom.Wait{}, false
		}
		for _, w := range due.Waits {
			if w.Name == wait {
				return w, true
			}
		}
		return loom.Wait{}, false
	}
	expiredTold := func() int {
		n := 0
		for _, told := range clock.OvenToldOf(oven.AggregateID.String()) {
			if _, ok := told.ExpiredWait(wait); ok {
				n++
			}
		}
		return n
	}
	dispatch := func(cmd loom.Command) {
		t.Helper()
		if err := cli.Dispatch(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}

	// heated: the wait starts, owned by loom, 2s out
	dispatch(&clockgen.HeatOven{CommandBase: oven})
	waitFor(t, ctx, "the overheat wait to start", func() bool { _, ok := held(); return ok })
	w, _ := held()
	due, _, _ := cli.LoopDue(ctx, "Oven", oven.Namespace, oven.AggregateID)
	if w.Owner != loom.DeadlineOwner || w.Deadline.Sub(due.Looked) != 2*time.Second {
		t.Errorf("the wait is %+v (looked %s), want loom's, 2s after the look", w, due.Looked)
	}

	// rested before 2s: the wait ends
	dispatch(&clockgen.RestOven{CommandBase: oven})
	waitFor(t, ctx, "the overheat wait to end", func() bool { _, ok := held(); return !ok })

	// heated again: a new wait; left heating: told once that it expired
	dispatch(&clockgen.HeatOven{CommandBase: oven})
	waitFor(t, ctx, "a new overheat wait", func() bool { _, ok := held(); return ok })
	waitFor(t, ctx, "the look to be told the deadline expired", func() bool { return expiredTold() == 1 })
	if _, ok := held(); ok {
		t.Error("the expired wait is still held")
	}
	// still heating, and looked at again for an event: no new wait, not told again
	dispatch(&clockgen.HeatOven{CommandBase: oven})
	looks := len(clock.OvenToldOf(oven.AggregateID.String()))
	waitFor(t, ctx, "a look for the event", func() bool { return len(clock.OvenToldOf(oven.AggregateID.String())) > looks })
	waitFor(t, ctx, "the event's look to be noted", func() bool {
		due, ok, err := cli.LoopDue(ctx, "Oven", oven.Namespace, oven.AggregateID)
		return err == nil && ok && !due.Event.After(due.Looked)
	})
	// that look read what fired from the row: a deadline started again
	// would be held now (loomtest's TestADeclaredDeadlineIsKeptInABubble
	// walks the hour after)
	if _, ok := held(); ok {
		t.Error("a fired deadline started again while the oven stayed heating")
	}
	if n := expiredTold(); n != 1 {
		t.Errorf("the look was told the deadline expired %d times, want once", n)
	}

	// rested, then heated: the deadline starts afresh
	dispatch(&clockgen.RestOven{CommandBase: oven})
	waitFor(t, ctx, "a look after resting", func() bool {
		told := clock.OvenToldOf(oven.AggregateID.String())
		return len(told) > looks+1
	})
	time.Sleep(1500 * time.Millisecond)
	dispatch(&clockgen.HeatOven{CommandBase: oven})
	waitFor(t, ctx, "the deadline to start afresh", func() bool { _, ok := held(); return ok })
	waitFor(t, ctx, "the second expiry", func() bool { return expiredTold() == 2 })
}
