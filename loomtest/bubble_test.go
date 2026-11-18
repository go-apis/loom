package loomtest_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	"github.com/go-apis/loom/loomtest"
)

// The tests here run the clock inside a synctest bubble: hours and weeks
// of looks and timers in no real time, in the same order every run.

// bubble runs f in a synctest bubble and fails when it took real time
// past budget: the point of the bubble is that it takes none.
func bubble(t *testing.T, budget time.Duration, f func(t *testing.T)) {
	t.Helper()
	began := time.Now()
	synctest.Test(t, f)
	took := time.Since(began)
	t.Logf("real time in the bubble: %s", took)
	if took > budget {
		t.Errorf("the bubble took %s of real time, want under %s", took, budget)
	}
}

// offsets is when each look was made, from start.
func offsets(start time.Time, looks []loomtest.LookMade) []time.Duration {
	out := make([]time.Duration, len(looks))
	for i, l := range looks {
		out[i] = l.At.Sub(start)
	}
	return out
}

func sameDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestALoopBacksOffAndParksInABubble: a kiln whose look fails is looked
// at again 1s, 2s and 4s apart, and parked at the failure limit, holding
// a wait on its operators; parked, it is left alone a day at a time, an
// event brings one look that fails and leaves it parked, and its
// operators' answer looks at it at once, and back on its minute.
func TestALoopBacksOffAndParksInABubble(t *testing.T) {
	bubble(t, 100*time.Millisecond, func(t *testing.T) {
		broken := true
		look := func(ctx context.Context) ([]loom.Command, error) {
			if broken {
				return nil, errors.New("the kiln is cracked")
			}
			return nil, nil
		}
		kiln := uuid.New()
		start := time.Now()
		loop := loomtest.Loop(t, clock.NewRegistry(), look).Instance("Kiln", "pottery", kiln).FailureLimit(4)

		loop.RunFor(7 * time.Second)
		if got, want := offsets(start, loop.Looks()), []time.Duration{0, time.Second, 3 * time.Second, 7 * time.Second}; !sameDurations(got, want) {
			t.Fatalf("looks at %v, want %v", got, want)
		}
		if !loop.Parked() || loop.Failures() != 4 || loop.State().LastError != "the kiln is cracked" {
			t.Fatalf("after 4 failures: %+v", loop.State())
		}
		var parkedOn loom.Wait
		for _, w := range loop.State().Waits {
			if w.Name == loom.LoopParkedWait {
				parkedOn = w
			}
		}
		if parkedOn.Owner != loom.LoopParkedOwner || !parkedOn.Deadline.Equal(start.Add(7*time.Second+loom.LoopParkFor)) {
			t.Fatalf("parked holding %+v", loop.State().Waits)
		}

		// parked: nothing for a day, then one look at the day, which
		// fails and waits another day
		loop.RunFor(2*loom.LoopParkFor - time.Second)
		if got, want := offsets(start, loop.Looks()[4:]), []time.Duration{7*time.Second + loom.LoopParkFor}; !sameDurations(got, want) {
			t.Fatalf("parked, looked at %v, want %v", got, want)
		}
		if !loop.Parked() || loop.Failures() != 5 {
			t.Fatalf("a failed look while parked: %+v", loop.State())
		}

		// an event brings one look within a second; it fails, still parked
		loop.Dispatch(&clockgen.StokeKiln{CommandBase: loom.CommandBase{Namespace: "pottery", AggregateID: kiln}})
		evented := time.Now()
		loop.RunFor(10 * time.Second)
		if n := len(loop.Looks()); n != 6 || !loop.Looks()[5].At.Equal(evented.Add(loom.LoopSoon)) || !loop.Looks()[5].Told.Event {
			t.Fatalf("after an event the looks are %v", offsets(start, loop.Looks()))
		}
		if !loop.Parked() || loop.Failures() != 6 {
			t.Fatalf("a failed look for an event unparked it: %+v", loop.State())
		}

		// fixed, and its operators answer: looked at at once, unparked,
		// and back on its minute
		broken = false
		answered := time.Now()
		loop.EndWait(loom.LoopParkedWait).RunFor(90 * time.Second)
		if got, want := offsets(answered, loop.Looks()[6:]), []time.Duration{0, time.Minute}; !sameDurations(got, want) {
			t.Fatalf("after the answer looked at %v, want %v", got, want)
		}
		if loop.Parked() || loop.Failures() != 0 || loop.State().LastError != "" {
			t.Fatalf("after the answer: %+v", loop.State())
		}
	})
}

// TestATimerADayOutFiresAtItsTime: a look schedules a command a day out
// (loom.After, read off the bubble's clock); it fires at that instant,
// not before, its event folds into the state, and the next look is told
// of it.
func TestATimerADayOutFiresAtItsTime(t *testing.T) {
	bubble(t, 2*time.Second, func(t *testing.T) {
		kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
		reader := &loomtest.Reader{}
		scheduled := false
		look := func(ctx context.Context) ([]loom.Command, error) {
			if !scheduled {
				scheduled = true
				return []loom.Command{loom.After(&clockgen.RecordKilnFired{CommandBase: kiln}, 24*time.Hour)}, nil
			}
			return nil, nil
		}
		start := time.Now()
		loop := loomtest.Loop(t, clock.NewRegistry(), look).Instance("Kiln", kiln.Namespace, kiln.AggregateID).Reading(reader)

		loop.RunFor(24*time.Hour - time.Second)
		if fired := loop.Timers().Fired(); len(fired) != 0 {
			t.Fatalf("fired before its day: %+v", fired)
		}
		if pending := loop.Timers().Pending(); len(pending) != 1 || !pending[0].At.Equal(start.Add(24*time.Hour)) {
			t.Fatalf("pending %+v, want one a day out", pending)
		}
		loop.RunFor(2 * time.Second)
		fired := loop.Timers().Fired()
		if len(fired) != 1 || !fired[0].FiredAt.Equal(start.Add(24*time.Hour)) || fired[0].Err != nil || len(fired[0].Events) != 1 {
			t.Fatalf("fired %+v, want one at 24h", fired)
		}
		if !reader.Aggregates["Kiln/pottery/"+kiln.AggregateID.String()].(*clockgen.Kiln).Fired {
			t.Error("the timer's event did not fold into the kiln")
		}
		// the timer fires before the look due at the same instant, which
		// is told its event
		last := loop.Looks()[len(loop.Looks())-1]
		if !last.Told.Event || !last.At.Equal(start.Add(24*time.Hour)) {
			t.Errorf("the look after the timer was at %s told %+v", last.At.Sub(start), last.Told)
		}
	})
}

// TestATimerTakenFromAReactionFiresAtItsTime: the timers on their own,
// as a reaction's scheduled commands are: one a day out fires at the
// day; one cancelled never does; one rescheduled under its key fires at
// its new time.
func TestATimerTakenFromAReactionFiresAtItsTime(t *testing.T) {
	bubble(t, 100*time.Millisecond, func(t *testing.T) {
		reg := clock.NewRegistry()
		a := loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}
		b := loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}
		start := time.Now()
		timers := loomtest.Timers(t, reg)
		rest := timers.Take(
			loom.After(&clockgen.StrikeAnvil{CommandBase: a}, 24*time.Hour),
			loom.After(&clockgen.StrikeAnvil{CommandBase: b}, time.Hour),
			&clockgen.RecordAnvilCool{CommandBase: a},
		)
		if len(rest) != 1 {
			t.Fatalf("Take left %d commands, want the unscheduled one", len(rest))
		}
		timers.Take(loom.CancelTimer(&clockgen.StrikeAnvil{CommandBase: b}))
		timers.RunFor(12 * time.Hour)
		// rescheduled under its key: the latest stands
		timers.Take(loom.At(&clockgen.StrikeAnvil{CommandBase: a}, start.Add(36*time.Hour)))
		timers.RunFor(23 * time.Hour)
		if len(timers.Fired()) != 0 {
			t.Fatalf("fired %+v before 36h", timers.Fired())
		}
		timers.RunFor(time.Hour)
		if fired := timers.Fired(); len(fired) != 1 || !fired[0].FiredAt.Equal(start.Add(36*time.Hour)) || fired[0].Err != nil {
			t.Fatalf("fired %+v, want one at 36h", fired)
		}
	})
}

// TestAWaitADayOutCallsItsLookAtTheDay: a look waits on a smith for a
// day. The loop is looked at every minute in between, told it is still
// waiting; at the day it is told the wait expired, once.
func TestAWaitADayOutCallsItsLookAtTheDay(t *testing.T) {
	bubble(t, 2*time.Second, func(t *testing.T) {
		started := false
		look := func(ctx context.Context) ([]loom.Command, error) {
			if !started {
				started = true
				return nil, loom.WaitFor(ctx, loom.Wait{Name: "smith", Owner: "user:smith", Deadline: loom.LookingAt(ctx).At.Add(24 * time.Hour)})
			}
			return nil, nil
		}
		start := time.Now()
		loop := loomtest.Loop(t, clock.NewRegistry(), look).Instance("Kiln", "pottery", uuid.New())
		loop.RunFor(3 * 24 * time.Hour)

		var expired []time.Duration
		for _, l := range loop.Looks() {
			if _, ok := l.Told.ExpiredWait("smith"); ok {
				expired = append(expired, l.At.Sub(start))
			}
		}
		if !sameDurations(expired, []time.Duration{24 * time.Hour}) {
			t.Fatalf("told the wait expired at %v, want once at 24h", expired)
		}
		// a look a minute through the day, and one at the deadline
		if n := len(loop.Looks()); n != 3*24*60+1 {
			t.Errorf("%d looks over three days, want one a minute", n)
		}
		if w := loop.State().Waits; len(w) != 0 {
			t.Errorf("a spent wait is still held: %+v", w)
		}
	})
}

// TestAnApprovalAWeekOutTakesItsDeadlineAction: a look asks for an
// approval due in a week, rejected at its deadline. Nobody answers: at
// the week it is rejected, by "deadline", and the look that is told acts
// on it. A second, answered in time by its role, settles at once and
// looks at the instance within that instant; an answer from someone
// without the role, or after the deadline, is refused.
func TestAnApprovalAWeekOutTakesItsDeadlineAction(t *testing.T) {
	bubble(t, 2*time.Second, func(t *testing.T) {
		week := 7 * 24 * time.Hour
		kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
		asking := func(ctx context.Context) ([]loom.Command, error) {
			told := loom.LookingAt(ctx)
			rec, ok := told.Approval("fire")
			if !ok {
				return nil, loom.AskApproval(ctx, loom.Approval{Name: "fire", What: "Fire the kiln's load", Role: "kiln-master", Deadline: told.At.Add(week), AtDeadline: loom.AtDeadlineReject})
			}
			switch rec.Status {
			case loom.ApprovalApproved:
				return []loom.Command{&clockgen.RecordKilnFired{CommandBase: kiln}}, nil
			case loom.ApprovalRejected:
				return []loom.Command{&clockgen.StokeKiln{CommandBase: kiln}}, nil
			}
			return nil, nil
		}

		start := time.Now()
		reader := &loomtest.Reader{}
		loop := loomtest.Loop(t, clock.NewRegistry(), asking).Instance("Kiln", kiln.Namespace, kiln.AggregateID).Reading(reader)
		loop.RunFor(week - time.Second)
		if rec, _ := loop.Approval("fire"); rec.Status != loom.ApprovalOpen {
			t.Fatalf("before the week the approval is %+v", rec)
		}
		loop.RunFor(time.Second)
		rec, _ := loop.Approval("fire")
		if rec.Status != loom.ApprovalRejected || rec.AnsweredBy != loom.ApprovalByDeadline || !rec.AnsweredAt.Equal(start.Add(week)) {
			t.Fatalf("at the week the approval is %+v", rec)
		}
		last := loop.Looks()[len(loop.Looks())-1]
		if _, ok := last.Told.ExpiredWait(loom.ApprovalWait("fire")); !ok || !last.At.Equal(start.Add(week)) || len(last.Events) != 1 {
			t.Fatalf("the look at the week: at %s told %+v emitted %v", last.At.Sub(start), last.Told, last.Events)
		}
		if _, err := loop.Answer("fire", "user:master", loom.ApprovalAnswer{Approve: true, Roles: []string{"kiln-master"}}); err == nil {
			t.Error("an answer after the deadline was taken")
		}

		// asked again, and answered in time by its role
		other := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
		kiln = other
		loop = loomtest.Loop(t, clock.NewRegistry(), asking).Instance("Kiln", other.Namespace, other.AggregateID).Reading(reader)
		loop.RunFor(time.Hour)
		if _, err := loop.Answer("fire", "user:potter", loom.ApprovalAnswer{Approve: true, Roles: []string{"potter"}}); !errors.Is(err, loom.ErrNotYourApproval) {
			t.Errorf("an answer from a potter: %v", err)
		}
		answered := time.Now()
		if _, err := loop.Answer("fire", "user:master", loom.ApprovalAnswer{Approve: true, Reason: "glaze is dry", Roles: []string{"kiln-master"}}); err != nil {
			t.Fatal(err)
		}
		loop.RunFor(time.Second)
		looks := loop.Looks()
		var fired *loomtest.LookMade
		for i := range looks {
			if len(looks[i].Events) > 0 {
				fired = &looks[i]
			}
		}
		if fired == nil || !fired.At.Equal(answered) {
			t.Fatalf("the answer was acted on at %v, want at once", fired)
		}
		if w := loop.State().Waits; len(w) != 0 {
			t.Errorf("an answered approval still holds its wait: %+v", w)
		}
	})
}

// TestADeclaredDeadlineIsKeptInABubble: an oven declares `deadline
// overheat after 2s while status in ("heating")`. Heated, it holds the
// wait; rested before 2s, it holds none; heated again and left heating,
// its look is told once that the deadline expired, at 2s, and no new
// wait starts while it stays heating, an hour on; rested and heated
// again, the deadline starts afresh.
func TestADeclaredDeadlineIsKeptInABubble(t *testing.T) {
	bubble(t, 100*time.Millisecond, func(t *testing.T) {
		oven := loom.CommandBase{Namespace: "bakery", AggregateID: uuid.New()}
		wait := loom.DeadlineWait("overheat")
		var expiredAt []time.Time
		look := func(ctx context.Context) ([]loom.Command, error) {
			if _, ok := loom.LookingAt(ctx).ExpiredWait(wait); ok {
				expiredAt = append(expiredAt, loom.LookingAt(ctx).At)
			}
			return nil, nil
		}
		loop := loomtest.Loop(t, clock.NewRegistry(), look).Instance("Oven", oven.Namespace, oven.AggregateID)
		held := func() (loom.Wait, bool) {
			for _, w := range loop.State().Waits {
				if w.Name == wait {
					return w, true
				}
			}
			return loom.Wait{}, false
		}

		// the first event makes the oven due at once
		heated := time.Now()
		loop.Dispatch(&clockgen.HeatOven{CommandBase: oven}).RunFor(loom.LoopSoon)
		if w, ok := held(); !ok || w.Owner != loom.DeadlineOwner || !w.Deadline.Equal(heated.Add(2*time.Second)) {
			t.Fatalf("heated, the oven holds %+v", loop.State().Waits)
		}
		loop.Dispatch(&clockgen.RestOven{CommandBase: oven}).RunFor(loom.LoopSoon)
		if _, ok := held(); ok {
			t.Fatal("rested, the oven still holds the deadline")
		}

		// (an event at the instant of a look lands a microsecond after it,
		// so its look is a second and a microsecond on: run two seconds)
		loop.Dispatch(&clockgen.HeatOven{CommandBase: oven}).RunFor(2 * loom.LoopSoon)
		w, ok := held()
		if !ok {
			t.Fatal("heated again, no deadline")
		}
		loop.RunFor(time.Hour)
		if len(expiredAt) != 1 || !expiredAt[0].Equal(w.Deadline) {
			t.Fatalf("told the deadline expired at %v, want once at %s", expiredAt, w.Deadline)
		}
		loop.Dispatch(&clockgen.HeatOven{CommandBase: oven}).RunFor(time.Hour)
		if _, ok := held(); ok || len(expiredAt) != 1 {
			t.Fatalf("a fired deadline started again while the oven stayed heating (told %d times)", len(expiredAt))
		}

		loop.Dispatch(&clockgen.RestOven{CommandBase: oven}).RunFor(2 * loom.LoopSoon)
		loop.Dispatch(&clockgen.HeatOven{CommandBase: oven}).RunFor(2 * loom.LoopSoon)
		w, ok = held()
		if !ok {
			t.Fatal("rested and heated, the deadline did not start afresh")
		}
		loop.RunFor(time.Minute)
		if len(expiredAt) != 2 || !expiredAt[1].Equal(w.Deadline) {
			t.Fatalf("told the deadline expired at %v, want a second time at %s", expiredAt, w.Deadline)
		}
	})
}

// TestLookDoneAndLookAgainInABubble: a look that says LookDone is looked
// at no more, until an event; one that asks LookAgain is looked at
// then, sooner than its minute. An event for a done instance starts its
// looks afresh, due at once.
func TestLookDoneAndLookAgainInABubble(t *testing.T) {
	bubble(t, 100*time.Millisecond, func(t *testing.T) {
		kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
		looks := 0
		look := func(ctx context.Context) ([]loom.Command, error) {
			looks++
			switch looks {
			case 1:
				loom.LookAgain(ctx, 5*time.Second)
			case 2:
				loom.LookDone(ctx)
			}
			return nil, nil
		}
		start := time.Now()
		loop := loomtest.Loop(t, clock.NewRegistry(), look).Instance("Kiln", kiln.Namespace, kiln.AggregateID)
		loop.RunFor(24 * time.Hour)
		if got, want := offsets(start, loop.Looks()), []time.Duration{0, 5 * time.Second}; !sameDurations(got, want) {
			t.Fatalf("looks at %v, want %v", got, want)
		}
		if !loop.Done() {
			t.Fatal("LookDone did not end the looks")
		}
		evented := time.Now()
		loop.Dispatch(&clockgen.StokeKiln{CommandBase: kiln}).RunFor(90 * time.Second)
		if got, want := offsets(evented, loop.Looks()[2:]), []time.Duration{0, time.Minute}; !sameDurations(got, want) {
			t.Fatalf("after an event looks at %v, want %v", got, want)
		}
	})
}

// gaps is how long after the look before it each look was made.
func gaps(looks []loomtest.LookMade) []time.Duration {
	out := make([]time.Duration, 0, len(looks))
	for i := 1; i < len(looks); i++ {
		out = append(out, looks[i].At.Sub(looks[i-1].At).Round(time.Millisecond))
	}
	return out
}

// TestALoopThatKeepsActingWithoutProgressIsBackedOffInABubble: a kiln
// whose look stokes it every time it is called, and changes nothing
// loom records about it, is not called flat out. Its own command's
// event brings the second look within a second, as ever; from there the
// looks are 2s, 4s, doubling to the minute apart, and an hour holds 65
// of them where it held 3,600. An event from outside is looked at
// within a second all the same and ends the run, and so does a look
// that changes what is recorded.
func TestALoopThatKeepsActingWithoutProgressIsBackedOffInABubble(t *testing.T) {
	bubble(t, 500*time.Millisecond, func(t *testing.T) {
		kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
		waiting := false
		look := func(ctx context.Context) ([]loom.Command, error) {
			if waiting {
				waiting = false
				if err := loom.WaitFor(ctx, loom.Wait{Name: "glaze", Owner: "potter", Deadline: time.Now().Add(time.Hour)}); err != nil {
					return nil, err
				}
			}
			return []loom.Command{&clockgen.StokeKiln{CommandBase: kiln}}, nil
		}
		loop := loomtest.Loop(t, clock.NewRegistry(), look).Instance("Kiln", kiln.Namespace, kiln.AggregateID)

		loop.RunFor(time.Hour)
		looks := loop.Looks()
		s := time.Second
		if got, want := gaps(looks)[:8], []time.Duration{s, 2 * s, 4 * s, 8 * s, 16 * s, 32 * s, 60 * s, 60 * s}; !sameDurations(got, want) {
			t.Fatalf("the looks are %v apart, want %v", got, want)
		}
		if n := len(looks); n > 65 {
			t.Fatalf("%d looks in an hour, want 65: seven on the way to the minute and one a minute after", n)
		}
		soon := 0
		for _, g := range gaps(looks) {
			if g < loom.LoopEvery {
				soon++
			}
		}
		if soon >= 20 {
			t.Fatalf("%d looks in the hour came sooner than the minute, want fewer than 20", soon)
		}
		for i, l := range looks {
			if l.State.Acting != i+1 {
				t.Fatalf("after look %d the loop has acted without progress %d times in a row, want %d", i+1, l.State.Acting, i+1)
			}
		}

		// an event from outside: looked at within the second, held back
		// by nothing, and the run starts over
		before := len(loop.Looks())
		evented := time.Now()
		loop.Dispatch(&clockgen.StokeKiln{CommandBase: kiln}).RunFor(5 * time.Second)
		after := loop.Looks()[before:]
		if len(after) != 3 {
			t.Fatalf("after an outside event the looks came %v after it, want three in five seconds", offsets(evented, after))
		}
		if at := after[0].At.Sub(evented).Round(time.Millisecond); at != loom.LoopSoon || !after[0].Told.Event {
			t.Fatalf("the look for an outside event came %s after it, want %s", at, loom.LoopSoon)
		}
		if after[0].State.Acting != 1 || after[1].State.Acting != 2 || after[2].State.Acting != 3 {
			t.Fatalf("after an outside event the run went %d, %d, %d; want 1, 2, 3", after[0].State.Acting, after[1].State.Acting, after[2].State.Acting)
		}
		if got, want := gaps(after), []time.Duration{s, 2 * s}; !sameDurations(got, want) {
			t.Fatalf("after an outside event the looks are %v apart, want %v", got, want)
		}

		// a look that changes what is recorded (it starts a wait) is
		// progress: the run starts over from it
		waiting = true
		before = len(loop.Looks())
		loop.RunFor(8 * time.Second)
		after = loop.Looks()[before:]
		if len(after) < 3 || after[0].State.Acting != 1 || after[1].State.Acting != 2 {
			t.Fatalf("after a look that started a wait: %d looks, the run at %d then %d; want it to start over", len(after), after[0].State.Acting, after[1].State.Acting)
		}
		if got := gaps(after)[0]; got != s {
			t.Fatalf("the look after one that made progress came %s later, want %s", got, s)
		}
	})
}

// TestALookThatAsksAgainWhileActingWithoutProgressIsHeldToTheBackoff:
// LookAgain is how a polling look keeps itself going, so it is held to
// the same backoff; a look that sends nothing asks as it always could.
func TestALookThatAsksAgainWhileActingWithoutProgressIsHeldToTheBackoff(t *testing.T) {
	bubble(t, 200*time.Millisecond, func(t *testing.T) {
		kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
		acting := true
		look := func(ctx context.Context) ([]loom.Command, error) {
			loom.LookAgain(ctx, 500*time.Millisecond)
			if !acting {
				return nil, nil
			}
			return []loom.Command{&clockgen.StokeKiln{CommandBase: kiln}}, nil
		}
		loop := loomtest.Loop(t, clock.NewRegistry(), look).Instance("Kiln", kiln.Namespace, kiln.AggregateID)
		loop.RunFor(10 * time.Second)
		half, s := 500*time.Millisecond, time.Second
		if got, want := gaps(loop.Looks()), []time.Duration{half, 2 * s, 4 * s}; !sameDurations(got, want) {
			t.Fatalf("a look that acts and asks again every half second is looked at %v apart, want %v", got, want)
		}
		acting = false
		before := len(loop.Looks())
		loop.RunFor(10 * time.Second)
		after := gaps(loop.Looks()[before:])
		if len(after) < 3 || after[1] != half || after[2] != half {
			t.Fatalf("a look that sends nothing and asks again every half second is looked at %v apart, want %s", after, half)
		}
	})
}
