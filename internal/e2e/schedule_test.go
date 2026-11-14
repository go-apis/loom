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

// TestSchedulesKeepTicking: a schedule dispatches its command at the
// deploy and every interval after, to the stream the schema names or
// derives. A tick the handler refuses does not end the series, which is
// what a command re-arming itself cannot promise. Status lists each
// with its next time, and a schedule the newest build no longer
// declares stops.
func TestSchedulesKeepTicking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	clock.Refuse.Store(0)
	clock.Refused.Store(0)
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry(), Generation: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// the streams: derived from the service and the schedule's name, and
	// the one the schema names
	bell := loom.ScheduleTarget("clock", "bell", "tower", "").AggregateID
	chime := uuid.MustParse("11111111-2222-4333-8444-555555555555")
	if again := loom.ScheduleTarget("clock", "bell", "tower", "").AggregateID; again != bell || bell == chime || bell == uuid.Nil {
		t.Fatalf("derived stream id %s, again %s", bell, again)
	}
	rings := func(id uuid.UUID) int64 {
		t.Helper()
		state, _, err := cli.Load(ctx, "Bell", "tower", id)
		if err != nil {
			t.Fatal(err)
		}
		return state.(*clockgen.Bell).Rings
	}

	started := time.Now()
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// both ring at the deploy; the one-second bell goes on, the hourly
	// chime has rung once
	waitFor(t, ctx, "the bell to ring three times", func() bool { return rings(bell) >= 3 })
	if took := time.Since(started); took < 1500*time.Millisecond || took > 8*time.Second {
		t.Errorf("three rings took %s; want the first at the start and one a second after", took)
	}
	if got := rings(chime); got != 1 {
		t.Errorf("the hourly chime has rung %d times; want once, at the start", got)
	}

	// two ticks refused by the handler: the series goes on
	before := rings(bell)
	clock.Refuse.Store(2)
	waitFor(t, ctx, "both refusals to be spent and the bell to ring again", func() bool {
		return clock.Refused.Load() == 2 && rings(bell) > before
	})
	dead, err := cli.DeadLetters(ctx, 10)
	if err != nil || len(dead) != 0 {
		t.Errorf("refused ticks left dead letters: %+v, %v", dead, err)
	}

	// status: each schedule, its interval and when it is next due
	st, err := cli.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Schedules) != 2 || st.Schedules[0].Name != "bell" || st.Schedules[0].Every != 1 || st.Schedules[0].Command != "RingBell" ||
		st.Schedules[1].Name != "chime" || st.Schedules[1].Every != 3600 || st.Schedules[0].NextAt == nil || st.Schedules[1].NextAt == nil {
		t.Fatalf("schedules = %+v", st.Schedules)
	}
	if until := time.Until(*st.Schedules[1].NextAt); until < 55*time.Minute || until > 61*time.Minute {
		t.Errorf("the chime is next due in %s; want about an hour", until)
	}

	// a newer build that no longer declares the bell: it stops, and the
	// chime it still declares keeps its time
	reg := clock.NewRegistry()
	reg.Schedules = reg.Schedules[1:]
	newer, err := loom.New(loom.Config{DB: pool, Registry: reg, Generation: 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := newer.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the bell's schedule to be gone", func() bool {
		st, err := newer.Status(ctx)
		return err == nil && st.Timers.Pending == 1
	})
	stopped := rings(bell)
	time.Sleep(2500 * time.Millisecond)
	if got := rings(bell); got != stopped {
		t.Errorf("the bell rang %d more times after its schedule was removed", got-stopped)
	}
	if got := rings(chime); got != 1 {
		t.Errorf("the chime has rung %d times; the newer build's start is not a new first tick", got)
	}
}
