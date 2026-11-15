package e2e_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// released builds a clock client that states its release.
func released(t *testing.T, pool *pgxpool.Pool, release int64) *loom.Client {
	t.Helper()
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry(), Release: release})
	if err != nil {
		t.Fatal(err)
	}
	return cli
}

func floorsOf(t *testing.T, ctx context.Context, cli *loom.Client) map[string]int64 {
	t.Helper()
	floors, err := cli.EventFloors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, f := range floors {
		out[f.EventType] = f.Release
	}
	return out
}

// TestEventFloorRecordsFirstRelease: the release that first writes an
// event type is that type's floor, kept with the event — a unit of work
// that rolls back leaves none — and a later release writing the same
// type does not move it. A type the log held before floors were kept
// gets a floor of zero from Migrate.
func TestEventFloorRecordsFirstRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clock.Brittle.Store(false)
	t.Cleanup(func() { clock.Brittle.Store(false) })

	pool := testDB(t, ctx)
	five := released(t, pool, 5)
	if err := five.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	anvil := loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}
	if err := five.Dispatch(ctx, &clockgen.StrikeAnvil{CommandBase: anvil}); err != nil {
		t.Fatal(err)
	}
	if got := floorsOf(t, ctx, five); len(got) != 1 || got["AnvilStruck"] != 5 {
		t.Fatalf("floors after release 5 struck the anvil: %v", got)
	}

	// a refused command writes no event, and no floor
	clock.Brittle.Store(true)
	if err := five.Dispatch(ctx, &clockgen.RecordAnvilCool{CommandBase: anvil}); err == nil {
		t.Fatal("a brittle anvil was recorded cool")
	}
	clock.Brittle.Store(false)
	if _, there := floorsOf(t, ctx, five)["AnvilCooled"]; there {
		t.Error("a unit of work that rolled back left a floor")
	}

	// release 9 writes the same type again, and a new one
	nine := released(t, pool, 9)
	other := loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}
	if err := nine.Dispatch(ctx, &clockgen.StrikeAnvil{CommandBase: other}); err != nil {
		t.Fatal(err)
	}
	if err := nine.Dispatch(ctx, &clockgen.RecordAnvilCool{CommandBase: other}); err != nil {
		t.Fatal(err)
	}
	// and again, now that its client has seen the floor is there
	if err := nine.Dispatch(ctx, &clockgen.StrikeAnvil{CommandBase: loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}}); err != nil {
		t.Fatal(err)
	}
	got := floorsOf(t, ctx, nine)
	if got["AnvilStruck"] != 5 {
		t.Errorf("AnvilStruck's floor is %d, want 5: the first write wins", got["AnvilStruck"])
	}
	if got["AnvilCooled"] != 9 {
		t.Errorf("AnvilCooled's floor is %d, want 9", got["AnvilCooled"])
	}

	// a log that predates floors: its types get a floor of zero, once
	if _, err := pool.Exec(ctx, `DELETE FROM loom_event_floors`); err != nil {
		t.Fatal(err)
	}
	if err := released(t, pool, 9).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := floorsOf(t, ctx, nine); len(got) != 2 || got["AnvilStruck"] != 0 || got["AnvilCooled"] != 0 {
		t.Errorf("floors seeded for a log that predates them: %v", got)
	}
}

// TestEventFloorChecksOnStart: a build of an older release than one that
// first wrote an event type in the store refuses to Start, naming the
// type and the release to start instead. The release that wrote it, a
// newer one, and a build that states no release all start; so does an
// old build against types of unknown origin.
func TestEventFloorChecksOnStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	five := released(t, pool, 5)
	if err := five.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := five.Dispatch(ctx, &clockgen.StrikeAnvil{CommandBase: loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}}); err != nil {
		t.Fatal(err)
	}

	start := func(release int64) error {
		t.Helper()
		run, stop := context.WithCancel(ctx)
		defer stop()
		return released(t, pool, release).Start(run, 50*time.Millisecond)
	}
	err := start(3)
	var floor *loom.FloorError
	if !errors.As(err, &floor) {
		t.Fatalf("release 3 started against a store release 5 wrote to: %v", err)
	}
	if floor.Release != 3 || floor.Floor != 5 || floor.EventType != "AnvilStruck" || floor.Others != 0 {
		t.Errorf("the refusal: %+v", floor)
	}
	if msg := err.Error(); !strings.Contains(msg, "release 5") || !strings.Contains(msg, "AnvilStruck") || !strings.Contains(msg, "release 3 cannot start") {
		t.Errorf("the refusal does not name the floor and the event type: %s", msg)
	}
	for _, release := range []int64{5, 9, 0} {
		if err := start(release); err != nil {
			t.Errorf("release %d did not start: %v", release, err)
		}
	}

	// a second type from a later release: the refusal names the newest
	// floor, and counts the rest
	nine := released(t, pool, 9)
	if err := nine.Dispatch(ctx, &clockgen.RingBell{CommandBase: loom.CommandBase{Namespace: "tower", AggregateID: uuid.New()}}); err != nil {
		t.Fatal(err)
	}
	if err := start(3); !errors.As(err, &floor) || floor.Floor != 9 || floor.EventType != "BellRang" || floor.Others != 1 {
		t.Errorf("release 3 against floors 5 and 9: %v", err)
	}
	if err := start(5); !errors.As(err, &floor) || floor.Floor != 9 || floor.Others != 0 {
		t.Errorf("release 5 against a floor of 9: %v", err)
	}

	// floors of zero hold nothing back
	if _, err := pool.Exec(ctx, `UPDATE loom_event_floors SET release = 0`); err != nil {
		t.Fatal(err)
	}
	if err := start(1); err != nil {
		t.Errorf("release 1 did not start against types of unknown origin: %v", err)
	}
}
