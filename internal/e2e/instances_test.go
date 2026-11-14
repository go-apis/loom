package e2e_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// countingOrder is orders' handler, counting the cancels it handles: a
// timer's command runs on the instance that fired it, so the count says
// which instance did.
type countingOrder struct {
	orders.Order
	cancels atomic.Int64
}

func (h *countingOrder) CancelOrder(ctx context.Context, state *ordersgen.Order, cmd *ordersgen.CancelOrder) ([]loom.DomainEvent, error) {
	h.cancels.Add(1)
	return h.Order.CancelOrder(ctx, state, cmd)
}

// countingSpend is orders' customerSpend fold, counting the events it
// folds: a projection's step runs on one instance, so the count says
// which.
type countingSpend struct {
	orders.CustomerSpend
	folds atomic.Int64
}

func (f *countingSpend) OnOrderPlaced(state *ordersgen.CustomerSpend, evt *loom.Event, data *ordersgen.OrderPlaced) error {
	f.folds.Add(1)
	return f.CustomerSpend.OnOrderPlaced(state, evt, data)
}

// TestTheNewestGenerationAliveDoesTheBackgroundWork: an instance fires
// timers and folds projections while no live instance is of a newer
// generation. Alone, the old build does both. Beside a newer build it
// does neither, and the newer one does. When the newer build's
// instances are gone — traffic shifted back — the old one takes both
// up again. Status lists the live builds.
func TestTheNewestGenerationAliveDoesTheBackgroundWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	was := orders.AutoCancelAfter
	orders.AutoCancelAfter = 700 * time.Millisecond
	t.Cleanup(func() { orders.AutoCancelAfter = was })

	pool := testDB(t, ctx)
	instance := func(generation int64, revision string) (*loom.Client, *countingOrder, *countingSpend) {
		t.Helper()
		h, spend := &countingOrder{}, &countingSpend{}
		impl := ordersgen.Impl{
			ShipOnPayment:      &orders.ShipOnPayment{},
			ScheduleAutoCancel: &orders.ScheduleAutoCancel{},
			DropAutoCancel:     &orders.DropAutoCancel{},
			CustomerSpend:      spend,
			Upcasts:            &orders.Upcasts{},
		}
		impl.Order(h)
		impl.Parcel(&orders.Parcel{})
		cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: ordersgen.NewRegistry(impl),
			Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local"), Generation: generation, Revision: revision})
		if err != nil {
			t.Fatal(err)
		}
		return cli, h, spend
	}
	older, olderOrders, olderSpend := instance(10, "orders-00010-old")
	newer, newerOrders, newerSpend := instance(20, "orders-00020-new")
	if err := older.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	place := func() {
		t.Helper()
		cmd := &ordersgen.PlaceOrder{CustomerId: uuid.New(), Currency: "USD", Items: []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 500}}}
		cmd.Namespace, cmd.AggregateID = "acme", uuid.New()
		if err := older.Dispatch(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	builds := func() []loom.InstanceStatus {
		t.Helper()
		st, err := older.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return st.Instances
	}

	// alone, the old build fires
	if err := older.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	place()
	waitFor(t, ctx, "the old build, alone, to fire the timer", func() bool { return olderOrders.cancels.Load() == 1 })
	waitFor(t, ctx, "the old build, alone, to fold the order", func() bool { return olderSpend.folds.Load() == 1 })
	if got := builds(); len(got) != 1 || got[0].Generation != 10 || got[0].Revision != "orders-00010-old" || got[0].Count != 1 || !got[0].Newest {
		t.Errorf("live builds with the old one alone = %+v", got)
	}

	// a newer build arrives: it fires, and the old one no longer does
	newerCtx, stopNewer := context.WithCancel(ctx)
	defer stopNewer()
	if err := newer.Start(newerCtx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "both builds to be seen alive", func() bool { return len(builds()) == 2 })
	if got := builds(); got[0].Generation != 20 || !got[0].Newest || got[1].Generation != 10 || got[1].Newest {
		t.Errorf("live builds after the deploy = %+v; want the new one first and newest", got)
	}
	// the old build learns at its next beat; from then it does none of it
	waitFor(t, ctx, "the old build to learn it is superseded", func() bool {
		st, err := older.Status(ctx)
		return err == nil && !st.HereNewest
	})
	place()
	waitFor(t, ctx, "the new build to fire the timer", func() bool { return newerOrders.cancels.Load() == 1 })
	if got := olderOrders.cancels.Load(); got != 1 {
		t.Errorf("the old build has handled %d cancels; it had handled 1 before the newer build arrived and should fire none beside it", got)
	}
	waitFor(t, ctx, "the new build to fold the order", func() bool { return newerSpend.folds.Load() == 1 })
	if got := olderSpend.folds.Load(); got != 1 {
		t.Errorf("the old build has folded %d orders; it had folded 1 before the newer build arrived and should fold none beside it", got)
	}

	// the newer build's instances go: the old one is the newest alive
	stopNewer()
	waitFor(t, ctx, "the old build to be the only one alive", func() bool { return len(builds()) == 1 })
	waitFor(t, ctx, "the old build to learn it is the newest again", func() bool {
		st, err := older.Status(ctx)
		return err == nil && st.HereNewest
	})
	place()
	waitFor(t, ctx, "the old build to fire again", func() bool { return olderOrders.cancels.Load() == 2 })
	waitFor(t, ctx, "the old build to fold again", func() bool { return olderSpend.folds.Load() == 2 })
	if got, folds := newerOrders.cancels.Load(), newerSpend.folds.Load(); got != 1 || folds != 1 {
		t.Errorf("the stopped build handled %d cancels and folded %d orders; want 1 and 1", got, folds)
	}
}

// TestAnOlderBuildStandsDownWhenANewerOneArrives: an older build does
// not wait for its next beat to learn of a newer one. With beats five
// seconds apart, it has stood down well inside one of them, because the
// newer build announces itself.
func TestAnOlderBuildStandsDownWhenANewerOneArrives(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	instance := func(generation int64) *loom.Client {
		t.Helper()
		cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(),
			Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local"), Generation: generation})
		if err != nil {
			t.Fatal(err)
		}
		return cli
	}
	older, newer := instance(10), instance(20)
	if err := older.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const poll = 5 * time.Second
	if err := older.Start(ctx, poll); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the old build to be seen alive", func() bool {
		st, err := older.Status(ctx)
		return err == nil && len(st.Instances) == 1 && st.HereNewest
	})
	arrived := time.Now()
	if err := newer.Start(ctx, poll); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the old build to stand down", func() bool {
		st, err := older.Status(ctx)
		return err == nil && !st.HereNewest
	})
	if took := time.Since(arrived); took > poll/2 {
		t.Errorf("the old build stood down %s after the newer one arrived; a beat is %s, and it should not have waited for one", took, poll)
	}
}
