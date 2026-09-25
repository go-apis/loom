package e2e_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// The timer runner once held its claim transaction — FOR UPDATE on the due
// rows — open across every fire. A fired command whose reactions re-armed
// the same key then waited on that row lock while holding the append
// lock, and the poller waited on the dispatch: a cycle Postgres cannot
// see, which hung every append in the namespace (runsheet, 2026-09-25).
// These tests pin claim -> commit -> fire (docs/adr/0002).

// cancelTimerKey is the default key loom.After(CancelOrder) arms under.
func cancelTimerKey(id uuid.UUID) string { return "CancelOrder/default/" + id.String() }

// timerOrders starts orders with the auto-cancel timer shortened and
// extra reactors added (withPolicy / withProcess), the harness pool
// handed to them so a reactor can probe the database mid-dispatch.
func timerOrders(t *testing.T, setup, run context.Context, extra func(reg *loom.Registry, pool *pgxpool.Pool)) (*pgxpool.Pool, *loom.Client) {
	t.Helper()
	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = 200 * time.Millisecond
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, setup)
	reg := orders.NewRegistry()
	if extra != nil {
		extra(reg, pool)
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: reg, Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(setup); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(run, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return pool, cli
}

func placeUnpaidOrder(t *testing.T, ctx context.Context, cli *loom.Client) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: id, Namespace: "default"},
		CustomerId:  uuid.New(),
		Currency:    "USD",
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 100}},
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// rearmOnCancel reacts to OrderCancelled by re-arming the very timer that
// fired it — same command, same target, so the same default key.
func rearmOnCancel(name string) *loom.ReactorDef {
	return &loom.ReactorDef{
		Name:   name,
		Events: []string{"OrderCancelled"},
		Subs:   []loom.SubscriptionDef{{Event: "OrderCancelled", Dispatches: []string{"CancelOrder"}}},
		React: func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
			return []loom.Command{loom.After(&ordersgen.CancelOrder{
				CommandBase: loom.CommandBase{AggregateID: evt.AggregateID, Namespace: evt.Namespace},
			}, time.Hour)}, nil
		},
	}
}

// TestTimerRearmSameKeyDoesNotHang reproduces the incident: the fired
// command's reaction re-arms the same key. As a policy the re-arm runs
// inside the fired command's transaction, after its append took the
// append lock — under the old claim that is a certain hang; as a process
// it re-arms right after, racing the poller's post-fire delete. Either
// way the fire must complete and the re-armed row must survive with its
// new fire_at, inside 5s.
func TestTimerRearmSameKeyDoesNotHang(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra func(reg *loom.Registry, pool *pgxpool.Pool)
	}{
		{"policy", func(reg *loom.Registry, _ *pgxpool.Pool) {
			reg.Policies = append(reg.Policies, rearmOnCancel("rearmOnCancel"))
		}},
		{"process", func(reg *loom.Registry, _ *pgxpool.Pool) {
			reg.Processes = append(reg.Processes, rearmOnCancel("rearmOnCancel"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setup, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelSetup()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pool, cli := timerOrders(t, setup, ctx, tc.extra)

			id := placeUnpaidOrder(t, ctx, cli)
			waitFor(t, ctx, "the fired CancelOrder to commit", func() bool {
				state, _, err := cli.Load(ctx, "Order", "default", id)
				return err == nil && state.(*ordersgen.Order).Status == "cancelled"
			})
			// the re-arm is an hour out: neither the lease (5m) nor the
			// original due time, and not deleted as the fired row
			waitFor(t, ctx, "the re-armed timer to rest with its new fire_at", func() bool {
				var fireAt time.Time
				err := pool.QueryRow(ctx, `SELECT fire_at FROM loom_timers WHERE service='orders' AND key=$1`,
					cancelTimerKey(id)).Scan(&fireAt)
				return err == nil && time.Until(fireAt) > 50*time.Minute
			})
			// and the poller is not wedged: it goes on firing
			other := placeUnpaidOrder(t, ctx, cli)
			waitFor(t, ctx, "the next timer to fire", func() bool {
				state, _, err := cli.Load(ctx, "Order", "default", other)
				return err == nil && state.(*ordersgen.Order).Status == "cancelled"
			})
			if ctx.Err() != nil {
				t.Fatalf("exceeded the 5s deadline: %v", ctx.Err())
			}
		})
	}
}

// TestTimerFailingFireIsParkedNotLost proves a timer whose command fails
// every attempt is parked to dead letters as runner 'timer' and its row
// cleared — loud, never silently gone, and parked once.
func TestTimerFailingFireIsParkedNotLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, cli := timerOrders(t, ctx, ctx, nil)

	// cancelling an order that was never placed fails every time
	ghost := uuid.New()
	if err := cli.Schedule(ctx, loom.After(&ordersgen.CancelOrder{
		CommandBase: loom.CommandBase{AggregateID: ghost, Namespace: "default"},
	}, 0)); err != nil {
		t.Fatal(err)
	}

	parked := func() []loom.DeadLetter {
		letters, err := cli.DeadLetters(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		return letters
	}
	waitFor(t, ctx, "the failing timer to park", func() bool { return len(parked()) > 0 })
	letters := parked()
	if len(letters) != 1 || letters[0].Runner != "timer" {
		t.Fatalf("dead letters: %+v", letters)
	}
	var env struct {
		Key  string `json:"timer_key"`
		Type string `json:"command_type"`
	}
	if err := json.Unmarshal(letters[0].Envelope, &env); err != nil || env.Key != cancelTimerKey(ghost) || env.Type != "CancelOrder" {
		t.Fatalf("parked envelope: %s (%v)", letters[0].Envelope, err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_timers WHERE service='orders'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("%d timer rows outlived the park", rows)
	}
	// nothing re-fires or parks twice afterwards
	time.Sleep(500 * time.Millisecond)
	if n := len(parked()); n != 1 {
		t.Fatalf("dead letters after settling: %d", n)
	}
}

// TestTimerClaimCommittedBeforeDispatch proves no claim transaction is
// open across a fire: while the fired command's dispatch runs, another
// connection can lock the claimed timer row with NOWAIT. Under the old
// claim that lock was the poller's until the whole batch had fired.
func TestTimerClaimCommittedBeforeDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	probed := false
	var probeErr error
	var leased time.Time
	probe := func(reg *loom.Registry, pool *pgxpool.Pool) {
		reg.Policies = append(reg.Policies, &loom.ReactorDef{
			Name:   "probeClaim",
			Events: []string{"OrderCancelled"},
			React: func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {
				// inside the fired CancelOrder's own transaction
				tx, err := pool.Begin(ctx)
				if err != nil {
					return nil, err
				}
				defer tx.Rollback(ctx)
				var fireAt time.Time
				err = tx.QueryRow(ctx, `
					SELECT fire_at FROM loom_timers WHERE service='orders' AND key=$1
					FOR UPDATE NOWAIT`, cancelTimerKey(evt.AggregateID)).Scan(&fireAt)
				mu.Lock()
				probed, probeErr, leased = true, err, fireAt
				mu.Unlock()
				return nil, nil
			},
		})
	}
	_, cli := timerOrders(t, ctx, ctx, probe)

	id := placeUnpaidOrder(t, ctx, cli)
	waitFor(t, ctx, "the fired CancelOrder to commit", func() bool {
		state, _, err := cli.Load(ctx, "Order", "default", id)
		return err == nil && state.(*ordersgen.Order).Status == "cancelled"
	})
	mu.Lock()
	defer mu.Unlock()
	if !probed {
		t.Fatal("the probe never ran during the fire")
	}
	if probeErr != nil {
		t.Fatalf("the claimed timer row was still locked during its dispatch: %v", probeErr)
	}
	// the row was there, leased (pushed out), not deleted before the fire
	if time.Until(leased) < time.Minute {
		t.Fatalf("claimed row's fire_at %v is not a lease", leased)
	}
}
