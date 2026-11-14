package e2e_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
)

// TestEveryCallsAFunctionOnAnInterval: Every is a schedule whose tick
// is a function. It is called at the start and every interval after, on
// one instance of the newest generation; a call that fails or panics
// does not end the series; Status lists it; and its name may not be
// taken twice.
func TestEveryCallsAFunctionOnAnInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	var olderCalls, newerCalls, failures atomic.Int64
	instance := func(generation int64, calls *atomic.Int64) *loom.Client {
		t.Helper()
		cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry(), Generation: generation})
		if err != nil {
			t.Fatal(err)
		}
		err = cli.Every("sweep", 300*time.Millisecond, func(ctx context.Context) error {
			switch n := calls.Add(1); {
			case n == 2:
				failures.Add(1)
				return fmt.Errorf("the sweep failed (scripted)")
			case n == 3:
				failures.Add(1)
				panic("the sweep panicked (scripted)")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return cli
	}
	older := instance(10, &olderCalls)
	if err := older.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// a name is one schedule's: the schema's, or a function's, once
	if err := older.Every("sweep", time.Second, func(context.Context) error { return nil }); err == nil {
		t.Error("a second function took the name sweep")
	}
	if err := older.Every("bell", time.Second, func(context.Context) error { return nil }); err == nil {
		t.Error("a function took the name of a schedule the schema declares")
	}
	if err := older.Every("", time.Second, nil); err == nil {
		t.Error("Every accepted no name and no function")
	}

	if err := older.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// through a failure and a panic, it goes on
	waitFor(t, ctx, "the function to be called five times", func() bool { return olderCalls.Load() >= 5 })
	if failures.Load() != 2 {
		t.Errorf("%d scripted failures were reached; want 2", failures.Load())
	}
	st, err := older.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var sweep *loom.ScheduleStatus
	for i := range st.Schedules {
		if st.Schedules[i].Name == "sweep" {
			sweep = &st.Schedules[i]
		}
	}
	if sweep == nil || sweep.Command != "" || sweep.Every != 0.3 || sweep.NextAt == nil {
		t.Errorf("status schedules = %+v; want sweep, a function, every 0.3s", st.Schedules)
	}

	// a newer build takes it over: one instance at a time, the newest
	newer := instance(20, &newerCalls)
	if err := newer.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the old build to stand down", func() bool {
		st, err := older.Status(ctx)
		return err == nil && !st.HereNewest
	})
	stopped := olderCalls.Load()
	waitFor(t, ctx, "the new build to call the function", func() bool { return newerCalls.Load() >= 5 })
	if got := olderCalls.Load(); got > stopped+1 {
		t.Errorf("the old build called the function %d more times beside a newer one", got-stopped)
	}
}
