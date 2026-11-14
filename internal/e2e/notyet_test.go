package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// TestNotYetLooksAgainUntilDone: a reaction that answers NotYet is
// looked at again later under its process's @retry — once per look, not
// three times at once — and finishes when it is ready. One that is
// never ready is parked when the looks run out, saying how often it
// looked. One with no @retry to look again with is parked at once,
// saying so.
func TestNotYetLooksAgainUntilDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	clock.HotFor.Store(0)
	clock.Looks.Store(0)
	clock.Impatience.Store(false)
	t.Cleanup(func() { clock.Impatience.Store(false); clock.HotFor.Store(0) })

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	strike := func() uuid.UUID {
		t.Helper()
		id := uuid.New()
		if err := cli.Dispatch(ctx, &clockgen.StrikeAnvil{CommandBase: loom.CommandBase{Namespace: "forge", AggregateID: id}}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	cool := func(id uuid.UUID) bool {
		t.Helper()
		state, _, err := cli.Load(ctx, "Anvil", "forge", id)
		if err != nil {
			t.Fatal(err)
		}
		return state.(*clockgen.Anvil).Cool
	}
	parked := func() []loom.DeadLetter {
		t.Helper()
		dead, err := cli.DeadLetters(ctx, 20)
		if err != nil {
			t.Fatal(err)
		}
		return dead
	}

	// hot for two looks: the third finds it cool. Three looks, no more —
	// a failure would have been tried three times before the first wait
	clock.HotFor.Store(2)
	anvil := strike()
	waitFor(t, ctx, "the anvil to be recorded cool", func() bool { return cool(anvil) })
	if got := clock.Looks.Load(); got != 3 {
		t.Errorf("the process looked %d times; want 3: twice hot, once cool", got)
	}
	if dead := parked(); len(dead) != 0 {
		t.Errorf("a wait that ended well left dead letters: %+v", dead)
	}
	waitFor(t, ctx, "the look's timer to be gone", func() bool {
		st, err := cli.Status(ctx)
		return err == nil && st.Timers.Pending == 2 // the two schedules' rows
	})

	// never cool: @retry(4, …) is the first look and four more, then a
	// dead letter that says how long it looked
	clock.Looks.Store(0)
	clock.HotFor.Store(1000)
	never := strike()
	waitFor(t, ctx, "the wait to run out", func() bool { return len(parked()) == 1 })
	dead := parked()[0]
	if dead.Runner != "process:coolAnvil" || !strings.Contains(dead.Error, "still not ready after looking 4 more times") || !strings.Contains(dead.Error, "the anvil is still hot") {
		t.Errorf("dead letter = %s: %s", dead.Runner, dead.Error)
	}
	if got := clock.Looks.Load(); got != 5 {
		t.Errorf("the process looked %d times before giving up; want 5", got)
	}
	if cool(never) {
		t.Error("an anvil that never cooled was recorded cool")
	}

	// no @retry: parked at once, and told why
	clock.HotFor.Store(0)
	clock.Impatience.Store(true)
	strike()
	waitFor(t, ctx, "the impatient process's event to be parked", func() bool { return len(parked()) == 2 })
	var said string
	for _, d := range parked() {
		if d.Runner == "process:impatient" {
			said = d.Error
		}
	}
	if !strings.Contains(said, "declares no @retry to look again with") {
		t.Errorf("the impatient process's dead letter says %q", said)
	}
}
