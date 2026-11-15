package loomtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	"github.com/go-apis/loom/loomtest"
)

// anvilLook is a look over one anvil, as a @loop aggregate's Look method
// would be written: it reads how the anvil stands and says the one next
// step. A struck anvil that has not been recorded cool is recorded
// cool; one whose smith has not answered by the wait's deadline is
// struck again; otherwise there is nothing to do.
type anvilLook struct {
	anvil loom.CommandBase
}

func (l anvilLook) LookAnvil(ctx context.Context) ([]loom.Command, error) {
	if _, late := loom.LookingAt(ctx).ExpiredWait("smith"); late {
		return []loom.Command{&clockgen.StrikeAnvil{CommandBase: l.anvil}}, nil
	}
	state, version, err := loom.Load(ctx, "Anvil", l.anvil.Namespace, l.anvil.AggregateID)
	if err != nil {
		return nil, err
	}
	if version == 0 || state.(*clockgen.Anvil).Cool {
		return nil, nil
	}
	return []loom.Command{&clockgen.RecordAnvilCool{CommandBase: l.anvil}}, nil
}

func struckAnvil(t *testing.T) (anvilLook, *loomtest.Reader) {
	t.Helper()
	clock.Brittle.Store(false)
	look := anvilLook{anvil: loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}}
	// struck, and not yet cool
	reader := (&loomtest.Reader{}).AddAggregate("Anvil", "forge", look.anvil.AggregateID, &clockgen.Anvil{Cool: false})
	return look, reader
}

var noon = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

// TestLoopIsLookedAtWithinAMinuteAndWithinASecondOfAnEvent: a declared
// loop with no event is looked at within 60s of its last look, and
// within 1s of an event that arrives after it. A loop never looked at
// is due at once, and an event already seen brings nothing forward.
func TestLoopIsLookedAtWithinAMinuteAndWithinASecondOfAnEvent(t *testing.T) {
	look, reader := struckAnvil(t)
	loop := func() *loomtest.LoopScenario {
		return loomtest.Loop(t, clock.NewRegistry(), look.LookAnvil).Reading(reader)
	}

	if next := loop().NextLook(); !next.IsZero() {
		t.Errorf("a loop never looked at is next looked at %s, want at once", next)
	}
	quiet := loop().LookedAt(noon).NextLook()
	if wait := quiet.Sub(noon); wait <= 0 || wait > 60*time.Second {
		t.Errorf("with no event the next look is %s after the last, want within 60s", wait)
	}
	event := noon.Add(20 * time.Second)
	after := loop().LookedAt(noon).EventAt(event).NextLook()
	if wait := after.Sub(event); wait < 0 || wait > time.Second {
		t.Errorf("after an event the next look is %s after it, want within 1s", wait)
	}
	// an event that arrived before the last look was seen by it
	seen := loop().LookedAt(noon).EventAt(noon.Add(-5 * time.Second)).NextLook()
	if !seen.Equal(quiet) {
		t.Errorf("an event already seen moves the next look to %s, want %s", seen, quiet)
	}
	// an event in the last second of the minute does not push the look
	// past the minute
	late := loop().LookedAt(noon).EventAt(noon.Add(59500 * time.Millisecond)).NextLook()
	if late.After(noon.Add(60 * time.Second)) {
		t.Errorf("a late event pushes the next look to %s, past the minute", late)
	}
	// the look itself is told an event arrived
	if told := loop().LookedAt(noon).EventAt(event).Look(after).Told(); !told.Event || !told.At.Equal(after) {
		t.Errorf("the look after an event was told %+v", told)
	}
}

// TestAnExpiredWaitCallsTheLoopWithTheWaitExpired: a loop waiting on
// someone is looked at when the wait's deadline passes, sooner than its
// minute, and the look is told the wait has expired. Before the
// deadline the look is told it is still waiting. Once told, the wait is
// done with: it brings no further look forward.
func TestAnExpiredWaitCallsTheLoopWithTheWaitExpired(t *testing.T) {
	look, reader := struckAnvil(t)
	reader.AddAggregate("Anvil", "forge", look.anvil.AggregateID, &clockgen.Anvil{Cool: true})
	deadline := noon.Add(10 * time.Second)
	smith := loom.Wait{Name: "smith", Owner: "user:smith", Deadline: deadline}
	loop := loomtest.Loop(t, clock.NewRegistry(), look.LookAnvil).Reading(reader).LookedAt(noon).Waiting(smith)

	if next := loop.NextLook(); !next.Equal(deadline) {
		t.Fatalf("a loop waiting until %s is next looked at %s", deadline, next)
	}
	// looked at early: still waiting, nothing to do
	early := noon.Add(5 * time.Second)
	if told := loop.Look(early).ThenNothing().Told(); len(told.Expired) != 0 || len(told.Waiting) != 1 {
		t.Errorf("before the deadline the look was told %+v", told)
	}
	if next := loop.NextLook(); !next.Equal(deadline) {
		t.Errorf("after an early look the next is %s, want the deadline %s", next, deadline)
	}
	// at the deadline: told the wait expired, and it acts on that
	told := loop.Look(deadline).Then(&clockgen.StrikeAnvil{CommandBase: look.anvil}).Told()
	if w, ok := told.ExpiredWait("smith"); !ok || w.Owner != "user:smith" || len(told.Waiting) != 0 {
		t.Errorf("at the deadline the look was told %+v", told)
	}
	// the wait is spent: the next look is the minute's
	if next := loop.NextLook(); !next.Equal(deadline.Add(60 * time.Second)) {
		t.Errorf("after the wait expired the next look is %s, want a minute on", next)
	}
}

// TestLookingTwiceOnUnchangedStateEmitsNothingNew: the first look at a
// struck anvil records it cool; a second look, with nothing changed but
// what the first did, dispatches nothing and emits nothing.
func TestLookingTwiceOnUnchangedStateEmitsNothingNew(t *testing.T) {
	look, reader := struckAnvil(t)
	loop := loomtest.Loop(t, clock.NewRegistry(), look.LookAnvil).Reading(reader)

	first := loop.Look(noon).Then(&clockgen.RecordAnvilCool{CommandBase: look.anvil}).Emitted()
	if len(first) != 1 || first[0].(loom.DomainEvent).LoomEvent() != "AnvilCooled" {
		t.Fatalf("the first look emitted %v, want AnvilCooled", first)
	}
	loop.Look(noon.Add(time.Minute)).ThenNothing()
	loop.Look(noon.Add(2 * time.Minute)).ThenNothing()
}
