package loomtest_test

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
	"github.com/go-apis/loom/loomtest"
)

// floorStore is a store on a real Postgres that release 5 has written an
// event type to, and a way to start a build of any release against it.
func floorStore(t *testing.T) (start func(release int64) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool := loomtest.Postgres(t)
	build := func(release int64) *loom.Client {
		t.Helper()
		cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry(), Release: release})
		if err != nil {
			t.Fatal(err)
		}
		return cli
	}
	five := build(5)
	if err := five.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := five.Dispatch(ctx, &clockgen.StrikeAnvil{CommandBase: loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}}); err != nil {
		t.Fatal(err)
	}
	return func(release int64) error {
		t.Helper()
		run, stop := context.WithCancel(ctx)
		defer stop()
		return build(release).Start(run, 50*time.Millisecond)
	}
}

// TestFloor_RefusesStaleStart: a binary that predates an event type the
// store holds refuses to start, and says which release to start.
func TestFloor_RefusesStaleStart(t *testing.T) {
	start := floorStore(t)
	err := start(3)
	var floor *loom.FloorError
	if !errors.As(err, &floor) {
		t.Fatalf("release 3 started against a store release 5 wrote to: %v", err)
	}
	if floor.Floor != 5 || floor.Release != 3 || floor.EventType != "AnvilStruck" {
		t.Errorf("the refusal: %+v", floor)
	}
	if msg := err.Error(); !strings.Contains(msg, "Start release 5 or newer") || !strings.Contains(msg, "AnvilStruck") {
		t.Errorf("the refusal does not name the floor release and the event type: %s", msg)
	}
}

// TestFloor_StartsAtOrAfterFloor: the release that first wrote the type,
// and any later one, starts.
func TestFloor_StartsAtOrAfterFloor(t *testing.T) {
	start := floorStore(t)
	for _, release := range []int64{5, 6, 90} {
		if err := start(release); err != nil {
			t.Errorf("release %d did not start: %v", release, err)
		}
	}
}

// TestPostgresGivesEachTestItsOwnDatabase: two calls answer two
// databases, so what one test writes another never reads.
func TestPostgresGivesEachTestItsOwnDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := func(pool *pgxpool.Pool) string {
		t.Helper()
		var db string
		if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&db); err != nil {
			t.Fatal(err)
		}
		return db
	}
	a, b := name(loomtest.Postgres(t)), name(loomtest.Postgres(t))
	if a == b || !strings.HasPrefix(a, "loom_test_") {
		t.Errorf("databases %q and %q", a, b)
	}
}
