package e2e_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// notesTold is every child note the oven's looks were told, in order.
func notesTold(oven uuid.UUID) []loom.ChildNote {
	var out []loom.ChildNote
	for _, told := range clock.OvenToldOf(oven.String()) {
		out = append(out, told.ChildNotes...)
	}
	return out
}

func countNotes(notes []loom.ChildNote, child uuid.UUID, what string) int {
	n := 0
	for _, note := range notes {
		if note.ID == child && note.What == what {
			n++
		}
	}
	return n
}

// TestAParentIsToldItsChildIsParkedAndRestartsIt: a loaf whose look keeps
// failing is parked after the limit, and its oven's look is told within
// about a second, with the loaf's last error, and sees it parked among
// its children. The oven restarts it: the loaf's failures and parking
// are cleared, it is looked at again at once, and its row records the
// restart.
func TestAParentIsToldItsChildIsParkedAndRestartsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, _ := loopClockWith(t, ctx, true, loom.Config{LoopFailureLimit: 2})
	t.Cleanup(func() { clock.SetLoafLook(nil); clock.SetOvenScript(nil) })

	var burnt atomic.Bool
	burnt.Store(true)
	clock.SetLoafLook(func(ctx context.Context) ([]loom.Command, error) {
		if burnt.Load() {
			return nil, errors.New("the loaf is burnt")
		}
		return nil, nil
	})
	var restart atomic.Bool
	clock.SetOvenScript(func(ctx context.Context) ([]loom.Command, error) {
		if restart.Load() {
			for _, ch := range loom.LookingAt(ctx).Children {
				if ch.Parked {
					loom.RestartChild(ctx, ch.ChildRef)
				}
			}
		}
		return nil, nil
	})

	oven := loom.CommandBase{Namespace: "bakery", AggregateID: uuid.New()}
	loaf := loom.CommandBase{Namespace: "bakery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.RestOven{CommandBase: oven}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Dispatch(ctx, &clockgen.BakeLoaf{CommandBase: loaf, OvenId: oven.AggregateID}); err != nil {
		t.Fatal(err)
	}

	// parked after two failures; the oven is told, with the last error
	waitFor(t, ctx, "the loaf to be parked", func() bool {
		due, ok, _ := cli.LoopDue(ctx, "Loaf", loaf.Namespace, loaf.AggregateID)
		return ok && due.Parked
	})
	parkedAt := time.Now()
	waitFor(t, ctx, "the oven to be told the loaf is parked", func() bool {
		return countNotes(notesTold(oven.AggregateID), loaf.AggregateID, loom.ChildParked) > 0
	})
	if took := time.Since(parkedAt); took > 3*time.Second {
		t.Errorf("the oven was told %s after the loaf was parked, want within about a second", took)
	}
	var note loom.ChildNote
	for _, n := range notesTold(oven.AggregateID) {
		if n.ID == loaf.AggregateID && n.What == loom.ChildParked {
			note = n
		}
	}
	if note.LastError != "the loaf is burnt" || note.Aggregate != "Loaf" {
		t.Errorf("the parked note is %+v, want the loaf with its last error", note)
	}
	due, _, _ := cli.LoopDue(ctx, "Loaf", loaf.Namespace, loaf.AggregateID)
	if due.Parent == nil || due.Parent.Aggregate != "Oven" || due.Parent.ID != oven.AggregateID {
		t.Errorf("the loaf's parent reads %+v, want the oven", due.Parent)
	}
	told := clock.OvenToldOf(oven.AggregateID.String())
	if last := told[len(told)-1]; len(last.Children) != 1 || !last.Children[0].Parked || last.Children[0].ID != loaf.AggregateID {
		t.Errorf("the oven's look saw children %+v, want the parked loaf", last.Children)
	}

	// fixed, and restarted by the oven's next look
	burnt.Store(false)
	restart.Store(true)
	looks := clock.LoafLooks(loaf.AggregateID.String())
	if err := cli.Dispatch(ctx, &clockgen.RestOven{CommandBase: oven}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the loaf to be restarted", func() bool {
		due, ok, _ := cli.LoopDue(ctx, "Loaf", loaf.Namespace, loaf.AggregateID)
		return ok && due.RestartedAt != nil
	})
	waitFor(t, ctx, "a look at the restarted loaf", func() bool { return clock.LoafLooks(loaf.AggregateID.String()) > looks })
	due, _, _ = cli.LoopDue(ctx, "Loaf", loaf.Namespace, loaf.AggregateID)
	if due.Parked || due.Failures != 0 || due.RestartedBy != "Oven/"+oven.AggregateID.String() {
		t.Errorf("after the restart the loaf reads parked %v, %d failures, restarted by %q", due.Parked, due.Failures, due.RestartedBy)
	}
	for _, w := range due.Waits {
		if w.Name == loom.LoopParkedWait {
			t.Error("the restarted loaf still waits on its operators")
		}
	}
	// told once: the parked note is not told again
	if n := countNotes(notesTold(oven.AggregateID), loaf.AggregateID, loom.ChildParked); n != 1 {
		t.Errorf("the oven was told the loaf was parked %d times, want once", n)
	}
}

// TestAParentIsToldItsChildFinishedOrItsWaitExpired: one loaf reaches its
// end, another's wait runs out unanswered; each brings the oven's look
// forward and is told to it once.
func TestAParentIsToldItsChildFinishedOrItsWaitExpired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)
	t.Cleanup(func() { clock.SetLoafLook(nil) })

	oven := loom.CommandBase{Namespace: "bakery", AggregateID: uuid.New()}
	done := loom.CommandBase{Namespace: "bakery", AggregateID: uuid.New()}
	proving := loom.CommandBase{Namespace: "bakery", AggregateID: uuid.New()}
	var waited atomic.Bool
	clock.SetLoafLook(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		state, _, err := loom.Load(ctx, "Loaf", told.Namespace, told.ID)
		if err != nil {
			return nil, err
		}
		if state.(*clockgen.Loaf).Status == "finished" {
			loom.LookDone(ctx)
			return nil, nil
		}
		if told.ID == proving.AggregateID && !waited.Swap(true) {
			return nil, loom.WaitFor(ctx, loom.Wait{Name: "proofing", Owner: "baker", Deadline: time.Now().Add(time.Second)})
		}
		return nil, nil
	})
	if err := cli.Dispatch(ctx, &clockgen.RestOven{CommandBase: oven}); err != nil {
		t.Fatal(err)
	}
	for _, loaf := range []loom.CommandBase{done, proving} {
		if err := cli.Dispatch(ctx, &clockgen.BakeLoaf{CommandBase: loaf, OvenId: oven.AggregateID}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, ctx, "a first look at each loaf", func() bool {
		return clock.LoafLooks(done.AggregateID.String()) > 0 && clock.LoafLooks(proving.AggregateID.String()) > 0
	})
	if err := cli.Dispatch(ctx, &clockgen.FinishLoaf{CommandBase: done}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the oven to be told the loaf is done", func() bool {
		return countNotes(notesTold(oven.AggregateID), done.AggregateID, loom.ChildDone) > 0
	})
	waitFor(t, ctx, "the oven to be told the proofing wait ran out", func() bool {
		return countNotes(notesTold(oven.AggregateID), proving.AggregateID, loom.ChildWaitExpired) > 0
	})
	// a few more looks of the oven: neither is told again
	if err := cli.Dispatch(ctx, &clockgen.RestOven{CommandBase: oven}); err != nil {
		t.Fatal(err)
	}
	looks := len(clock.OvenToldOf(oven.AggregateID.String()))
	waitFor(t, ctx, "another look at the oven", func() bool { return len(clock.OvenToldOf(oven.AggregateID.String())) > looks })
	notes := notesTold(oven.AggregateID)
	if n := countNotes(notes, done.AggregateID, loom.ChildDone); n != 1 {
		t.Errorf("the oven was told the loaf was done %d times, want once", n)
	}
	if n := countNotes(notes, proving.AggregateID, loom.ChildWaitExpired); n != 1 {
		t.Errorf("the oven was told the proofing wait ran out %d times, want once", n)
	}
	if _, ok, _ := cli.LoopDue(ctx, "Loaf", done.Namespace, done.AggregateID); ok {
		t.Error("the finished loaf still has looks due")
	}
}
