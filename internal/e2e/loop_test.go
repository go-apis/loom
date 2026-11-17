package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// loopClock is a started clock service on a fresh store.
func loopClock(t *testing.T, ctx context.Context, start bool) (*loom.Client, *pgxpool.Pool) {
	t.Helper()
	return loopClockWith(t, ctx, start, loom.Config{})
}

// loopClockWith is loopClock with cfg's settings (its DB and Registry
// are the clock's).
func loopClockWith(t *testing.T, ctx context.Context, start bool, cfg loom.Config) (*loom.Client, *pgxpool.Pool) {
	t.Helper()
	clock.SetKilnLook(nil)
	t.Cleanup(func() { clock.SetKilnLook(nil) })
	pool := testDB(t, ctx)
	cfg.DB, cfg.Registry = pool, clock.NewRegistry()
	cli, err := loom.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if start {
		if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	return cli, pool
}

func dueOf(t *testing.T, ctx context.Context, cli *loom.Client, kiln loom.CommandBase) loom.LoopDue {
	t.Helper()
	due, ok, err := cli.LoopDue(ctx, "Kiln", kiln.Namespace, kiln.AggregateID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("kiln %s has no look due", kiln.AggregateID)
	}
	return due
}

// TestLoopDueFollowsTheInstance: creating an instance of a @loop
// aggregate writes when it is next looked at, no more than a minute out
// — at once, in fact, since it has never been looked at. An event for
// it brings the next look to within a second, with no minute waited. A
// unit of work that rolls back moves nothing, and an aggregate with no
// loop has no row.
func TestLoopDueFollowsTheInstance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli, pool := loopClock(t, ctx, false) // no runner: the rows stand as written

	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if _, ok, err := cli.LoopDue(ctx, "Kiln", kiln.Namespace, kiln.AggregateID); err != nil || ok {
		t.Fatalf("a kiln never lit has a look due: %v %v", ok, err)
	}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	created := dueOf(t, ctx, cli, kiln)
	if wait := time.Until(created.NextLook); wait > 60*time.Second {
		t.Errorf("a new kiln is next looked at in %s, want within 60s", wait)
	}
	if !created.Looked.IsZero() || created.Event.IsZero() {
		t.Errorf("a new kiln's row: %+v", created)
	}

	// as the runner leaves it after a look with nothing to do: a minute on
	later := time.Now().Add(time.Minute)
	if _, err := pool.Exec(ctx, `UPDATE loom_loop_due SET looked_at = now(), next_look = $1`, later); err != nil {
		t.Fatal(err)
	}
	// an event for the instance: the next look is within a second of it
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	after := dueOf(t, ctx, cli, kiln)
	if wait := time.Until(after.NextLook); wait > time.Second {
		t.Errorf("after an event the kiln is next looked at in %s, want within 1s", wait)
	}
	if !after.Event.After(after.Looked) {
		t.Errorf("the event is not noted as arrived since the last look: %+v", after)
	}
	if !after.Due(time.Now().Add(time.Second)) {
		t.Error("the rule does not call the kiln due within a second of its event")
	}

	// a refused command appends nothing and moves nothing
	if _, err := pool.Exec(ctx, `UPDATE loom_loop_due SET looked_at = now(), next_look = $1`, later); err != nil {
		t.Fatal(err)
	}
	clock.Refuse.Store(1)
	t.Cleanup(func() { clock.Refuse.Store(0) })
	bell := loom.CommandBase{Namespace: "tower", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}, &clockgen.RingBell{CommandBase: bell}); err == nil {
		t.Fatal("a unit of work with a refused command committed")
	}
	if got := dueOf(t, ctx, cli, kiln); !got.NextLook.Equal(later.Truncate(time.Microsecond)) && got.NextLook.Before(later.Add(-time.Second)) {
		t.Errorf("a rolled-back event moved the next look to %s", got.NextLook)
	}
	// an aggregate with no loop has no row
	if err := cli.Dispatch(ctx, &clockgen.RingBell{CommandBase: bell}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cli.LoopDue(ctx, "Bell", bell.Namespace, bell.AggregateID); err != nil || ok {
		t.Errorf("a bell, which has no loop, has a look due: %v %v", ok, err)
	}
}

// TestLoopLooksAtEveryLiveInstance: the runner looks at each instance
// soon after it is created and soon after each event, telling the look
// which instance it is and whether an event arrived. What the look
// dispatches is run, and its own events bring one more look, which
// finds nothing left to do. A look that says it is done is not looked
// at again until an event arrives for it.
func TestLoopLooksAtEveryLiveInstance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, pool := loopClock(t, ctx, true)

	// the look fires a lit kiln once it has been stoked twice, and is
	// done once it is fired
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		state, _, err := loom.Load(ctx, "Kiln", told.Namespace, told.ID)
		if err != nil {
			return nil, err
		}
		kiln := state.(*clockgen.Kiln)
		switch {
		case kiln.Fired:
			loom.LookDone(ctx)
			return nil, nil
		case kiln.Stokes >= 2:
			return []loom.Command{&clockgen.RecordKilnFired{CommandBase: loom.CommandBase{Namespace: told.Namespace, AggregateID: told.ID}}}, nil
		}
		return nil, nil
	})
	fired := func(kiln loom.CommandBase) bool {
		t.Helper()
		state, _, err := cli.Load(ctx, "Kiln", kiln.Namespace, kiln.AggregateID)
		if err != nil {
			t.Fatal(err)
		}
		return state.(*clockgen.Kiln).Fired
	}
	looks := func(kiln loom.CommandBase) int {
		n, _ := clock.KilnLooks(kiln.AggregateID.String())
		return n
	}

	a := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	b := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	for _, kiln := range []loom.CommandBase{a, b} {
		if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
			t.Fatal(err)
		}
	}
	// each is looked at soon after it is lit, and told which it is
	waitFor(t, ctx, "a first look at each kiln", func() bool { return looks(a) >= 1 && looks(b) >= 1 })
	if _, told := clock.KilnLooks(a.AggregateID.String()); told.Aggregate != "Kiln" || told.Namespace != "pottery" || told.ID != a.AggregateID || !told.Event {
		t.Errorf("the first look at kiln a was told %+v", told)
	}
	if fired(a) || fired(b) {
		t.Fatal("a kiln was fired before it was stoked")
	}

	// stoke a twice: the look after the second fires it, well inside a minute
	began := time.Now()
	for i := 0; i < 2; i++ {
		if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: a}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, ctx, "kiln a to be fired by its loop", func() bool { return fired(a) })
	if took := time.Since(began); took > 10*time.Second {
		t.Errorf("the loop took %s to act on an event", took)
	}
	if fired(b) {
		t.Error("the loop fired a kiln that was not stoked")
	}
	// the events of what the loop did were written by it
	var cause string
	if err := pool.QueryRow(ctx, `SELECT causation_id FROM loom_events WHERE type='KilnFired' AND aggregate_id=$1`, a.AggregateID).Scan(&cause); err != nil || cause != "loop:Kiln/"+a.AggregateID.String() {
		t.Errorf("KilnFired's cause is %q (%v), want the loop", cause, err)
	}

	// fired, the kiln is done: its row goes, and its looks stop
	waitFor(t, ctx, "kiln a's looks to end", func() bool {
		_, ok, err := cli.LoopDue(ctx, "Kiln", a.Namespace, a.AggregateID)
		return err == nil && !ok
	})
	settled := looks(a)
	time.Sleep(1500 * time.Millisecond)
	if again := looks(a); again != settled {
		t.Errorf("a kiln that is done was looked at %d more times", again-settled)
	}
	// kiln b, never done, is still due within its minute
	if due := dueOf(t, ctx, cli, b); time.Until(due.NextLook) > 60*time.Second || due.Looked.IsZero() {
		t.Errorf("kiln b's next look: %+v", due)
	}
	// an event for a done kiln starts its looks again
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: a}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "a look at kiln a after a new event", func() bool { return looks(a) > settled })
}

// TestLoopRecoversAPanickingLook: a look that panics does not take the
// process down. The panic is recorded as the instance's last error and
// counted as a failure, its next look is backed off, and the runner goes
// on looking at the service's other instances.
func TestLoopRecoversAPanickingLook(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)

	cracked := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	sound := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		if loom.LookingAt(ctx).ID == cracked.AggregateID {
			panic("the kiln cracked")
		}
		return nil, nil
	})
	looks := func(kiln loom.CommandBase) int {
		n, _ := clock.KilnLooks(kiln.AggregateID.String())
		return n
	}
	for _, kiln := range []loom.CommandBase{cracked, sound} {
		if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, ctx, "the cracked kiln's look to fail", func() bool { return dueOf(t, ctx, cli, cracked).Failures >= 1 })
	due := dueOf(t, ctx, cli, cracked)
	if !strings.Contains(due.LastError, "panicked") || !strings.Contains(due.LastError, "the kiln cracked") {
		t.Errorf("the cracked kiln's last error is %q, want the panic", due.LastError)
	}
	if wait := due.NextLook.Sub(due.Looked); wait > loom.LoopBackoff(due.Failures) {
		t.Errorf("after %d failures the next look is %s after the last, want within %s", due.Failures, wait, loom.LoopBackoff(due.Failures))
	}

	// the runner is still looking: the sound kiln, stoked after the
	// panic, is looked at again and has failed nothing
	seen := looks(sound)
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: sound}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "a look at the sound kiln after the panic", func() bool { return looks(sound) > seen })
	if d := dueOf(t, ctx, cli, sound); d.Failures != 0 || d.LastError != "" {
		t.Errorf("the sound kiln reads %d failures (%q), want none", d.Failures, d.LastError)
	}
	// and the cracked kiln is tried again, and fails again
	waitFor(t, ctx, "the cracked kiln's second failure", func() bool { return dueOf(t, ctx, cli, cracked).Failures >= 2 })
}

// TestLoopBacksOffAndCountsFailures: a look that fails three times and
// then succeeds reads 1, 2, 3 failures in a row, with its next look 1s,
// 2s, then 4s after each, and none once it succeeds — back to its
// minute.
func TestLoopBacksOffAndCountsFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)

	var mu sync.Mutex
	failing := 3
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		mu.Lock()
		defer mu.Unlock()
		if failing > 0 {
			failing--
			return nil, errors.New("the kiln is cold")
		}
		return nil, nil
	})
	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 3; want++ {
		waitFor(t, ctx, fmt.Sprintf("failure %d in a row", want), func() bool { return dueOf(t, ctx, cli, kiln).Failures >= want })
		due := dueOf(t, ctx, cli, kiln)
		if due.Failures != want {
			t.Fatalf("read %d failures in a row, want %d", due.Failures, want)
		}
		if wait, backoff := due.NextLook.Sub(due.Looked), time.Second<<(want-1); wait != backoff {
			t.Errorf("after failure %d the next look is %s after it, want %s", want, wait, backoff)
		}
		if due.LastError != "the kiln is cold" || due.Parked {
			t.Errorf("after failure %d: last error %q, parked %v", want, due.LastError, due.Parked)
		}
	}
	waitFor(t, ctx, "the look that succeeds", func() bool { return dueOf(t, ctx, cli, kiln).Failures == 0 })
	due := dueOf(t, ctx, cli, kiln)
	if due.LastError != "" {
		t.Errorf("after a look that succeeded the last error is %q", due.LastError)
	}
	if wait := due.NextLook.Sub(due.Looked); wait != loom.LoopEvery {
		t.Errorf("after a look that succeeded the next look is %s after it, want the minute", wait)
	}
}

// TestLoopParksPastTheFailureLimit: a look that always fails is parked
// once Config.LoopFailureLimit fail in a row, with a wait on its
// operators. Parked, it is not looked at again on its own; an event
// brings one look (which fails, and it stays parked), and ending the
// operators' wait clears the count and looks at it at once. LoopDue and
// Status both report it parked.
func TestLoopParksPastTheFailureLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, _ := loopClockWith(t, ctx, true, loom.Config{LoopFailureLimit: 3})

	var mu sync.Mutex
	broken := true
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		mu.Lock()
		defer mu.Unlock()
		if broken {
			return nil, errors.New("the kiln is cracked")
		}
		return nil, nil
	})
	looks := func(kiln loom.CommandBase) int {
		n, _ := clock.KilnLooks(kiln.AggregateID.String())
		return n
	}
	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the kiln to be parked", func() bool { return dueOf(t, ctx, cli, kiln).Parked })
	due := dueOf(t, ctx, cli, kiln)
	if due.Failures != 3 || due.LastError != "the kiln is cracked" {
		t.Errorf("parked with %d failures (%q), want 3", due.Failures, due.LastError)
	}
	var parkedOn *loom.Wait
	for i, w := range due.Waits {
		if w.Name == loom.LoopParkedWait {
			parkedOn = &due.Waits[i]
		}
	}
	if parkedOn == nil || parkedOn.Owner != loom.LoopParkedOwner {
		t.Fatalf("parked holding waits %+v, want %s owned by %s", due.Waits, loom.LoopParkedWait, loom.LoopParkedOwner)
	}
	if got := due.NextLook.Sub(due.Looked); got != loom.LoopParkFor {
		t.Errorf("parked, the next look is %s after the last, want %s", got, loom.LoopParkFor)
	}
	st, err := cli.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Loops.Parked != 1 || len(st.Loops.ParkedLoops) != 1 || st.Loops.ParkedLoops[0].ID != kiln.AggregateID || !st.Loops.ParkedLoops[0].Parked {
		t.Errorf("Status.Loops reads %+v, want the kiln parked", st.Loops)
	}

	// parked, it is not looked at on its own: well past the 4s its
	// backoff would have given, nothing has moved
	seen := looks(kiln)
	time.Sleep(6 * time.Second)
	if n, d := looks(kiln), dueOf(t, ctx, cli, kiln); n != seen || d.Failures != 3 {
		t.Fatalf("parked, the kiln was looked at %d more times (%d failures)", n-seen, d.Failures)
	}

	// an event brings one look; it fails, and the kiln stays parked
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "a look for the event", func() bool { return looks(kiln) > seen })
	waitFor(t, ctx, "the event's look to be noted", func() bool { return dueOf(t, ctx, cli, kiln).Failures == 4 })
	if d := dueOf(t, ctx, cli, kiln); !d.Parked {
		t.Error("a failed look for an event unparked the kiln")
	}

	// its operators fix it and answer the wait: it is looked at at once,
	// succeeds, and is back on its minute
	mu.Lock()
	broken = false
	mu.Unlock()
	seen = looks(kiln)
	if err := cli.EndWait(ctx, "Kiln", kiln.Namespace, kiln.AggregateID, loom.LoopParkedWait); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "a look once the operators answered", func() bool { return looks(kiln) > seen })
	waitFor(t, ctx, "the kiln to be unparked", func() bool {
		d := dueOf(t, ctx, cli, kiln)
		return !d.Parked && d.Failures == 0 && d.LastError == ""
	})
	if st, err := cli.Status(ctx); err != nil || st.Loops.Parked != 0 {
		t.Errorf("after the answer Status.Loops reads %+v (%v), want nothing parked", st.Loops, err)
	}
}
