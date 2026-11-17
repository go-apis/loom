package e2e_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/gpub"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// countingRaise is billing's reaction to another service's event,
// counting the events it handles: on an in-process bus that is the
// instance whose relay published them.
type countingRaise struct {
	billing.RaiseOnOrder
	handled atomic.Int64
}

func (h *countingRaise) OnOrderPlaced(ctx context.Context, evt *loom.Event, data *billinggen.OrderPlaced) ([]loom.Command, error) {
	h.handled.Add(1)
	return h.RaiseOnOrder.OnOrderPlaced(ctx, evt, data)
}

func billingRegistry(raise billinggen.RaiseOnOrderReactions) *loom.Registry {
	impl := billinggen.Impl{}
	impl.PostLedger(&billing.PostLedger{})
	impl.RaiseOnOrder(raise)
	impl.CaptureOnPaid(&billing.CaptureOnPaid{})
	impl.Invoice(&billing.Invoice{})
	impl.Payee(&billing.Payee{})
	impl.Contact(&billing.Contact{})
	impl.LedgerEntry(&billing.LedgerEntry{})
	return billinggen.NewRegistry(impl)
}

// process is one build of a binary that runs both services on a bus of
// its own, as a single-binary deployment does.
type process struct {
	orders, billing *loom.Client
	raise           *countingRaise
	stop            context.CancelFunc
}

func startProcess(t *testing.T, ctx context.Context, generation int64, bus loom.Bus, start bool) *process {
	t.Helper()
	p := &process{raise: &countingRaise{}}
	pool := sharedPool(t, ctx)
	var err error
	p.orders, err = loom.New(loom.Config{DB: pool, Bus: bus, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local"), Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	p.billing, err = loom.New(loom.Config{DB: pool, Bus: bus, Registry: billingRegistry(p.raise), Keys: testKeys(t), Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	if !start {
		return p
	}
	run, stop := context.WithCancel(ctx)
	p.stop = stop
	for _, cli := range []*loom.Client{p.orders, p.billing} {
		if err := cli.Start(run, 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

// sharedPool is one database for every process of a test.
var (
	sharedPools   = map[*testing.T]*pgxpool.Pool{}
	sharedPoolsMu sync.Mutex
)

func sharedPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	sharedPoolsMu.Lock()
	defer sharedPoolsMu.Unlock()
	if pool, ok := sharedPools[t]; ok {
		return pool
	}
	pool := testDB(t, ctx)
	sharedPools[t] = pool
	t.Cleanup(func() {
		sharedPoolsMu.Lock()
		delete(sharedPools, t)
		sharedPoolsMu.Unlock()
	})
	return pool
}

func placeAcmeOrder(t *testing.T, ctx context.Context, cli *loom.Client) uuid.UUID {
	t.Helper()
	cmd := &ordersgen.PlaceOrder{CustomerId: uuid.New(), Currency: "USD", Items: []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 500}}}
	cmd.Namespace, cmd.AggregateID = "acme", uuid.New()
	if err := cli.Dispatch(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	return cmd.AggregateID
}

// TestTheNewestBuildPublishesAndConsumes: two builds of a single-binary
// deployment, each with its own in-process bus, on one database. An
// event is published by the newest build's relay, so the newest build's
// consumer handles it; the older build's handles none while a newer one
// is alive, and takes the work back when it is gone.
func TestTheNewestBuildPublishesAndConsumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	migrator := startProcess(t, ctx, 0, loom.NewMemoryBus(), false)
	if err := migrator.orders.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := migrator.billing.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	invoiced := func(order uuid.UUID) bool {
		_, version, err := migrator.billing.Load(ctx, "Invoice", "acme", order)
		return err == nil && version > 0
	}

	older := startProcess(t, ctx, 10, loom.NewMemoryBus(), true)
	first := placeAcmeOrder(t, ctx, older.orders)
	waitFor(t, ctx, "the old build, alone, to raise the invoice", func() bool { return invoiced(first) })
	if older.raise.handled.Load() != 1 {
		t.Fatalf("the old build handled %d events alone; want 1", older.raise.handled.Load())
	}

	newer := startProcess(t, ctx, 20, loom.NewMemoryBus(), true)
	for _, cli := range []*loom.Client{older.orders, older.billing} {
		waitFor(t, ctx, "the old build to stand down", func() bool {
			st, err := cli.Status(ctx)
			return err == nil && !st.HereNewest
		})
	}
	// dispatched through the old build, as a request still routed to it
	// would be: the event is the newest build's to publish and handle
	second := placeAcmeOrder(t, ctx, older.orders)
	waitFor(t, ctx, "the new build to raise the invoice", func() bool { return invoiced(second) })
	if got := newer.raise.handled.Load(); got != 1 {
		t.Errorf("the new build handled %d events; want 1", got)
	}
	if got := older.raise.handled.Load(); got != 1 {
		t.Errorf("the old build has handled %d events; it handled 1 before the newer build arrived and should handle none beside it", got)
	}

	newer.stop()
	for _, cli := range []*loom.Client{older.orders, older.billing} {
		waitFor(t, ctx, "the old build to be the newest again", func() bool {
			st, err := cli.Status(ctx)
			return err == nil && st.HereNewest
		})
	}
	third := placeAcmeOrder(t, ctx, older.orders)
	waitFor(t, ctx, "the old build to raise the invoice again", func() bool { return invoiced(third) })
	if got := older.raise.handled.Load(); got != 2 {
		t.Errorf("the old build has handled %d events after the newer one went; want 2", got)
	}
}

// handBus is a transport shared by every instance of a consumer group,
// as Pub/Sub is: Publish records, and the test delivers an envelope to
// the instance it chooses.
type handBus struct {
	mu        sync.Mutex
	published []*loom.Envelope
	handlers  map[string]func(ctx context.Context, env *loom.Envelope) error
}

func (b *handBus) Publish(ctx context.Context, env *loom.Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, env)
	return nil
}

func (b *handBus) Subscribe(ctx context.Context, group string, h func(ctx context.Context, env *loom.Envelope) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.handlers == nil {
		b.handlers = map[string]func(ctx context.Context, env *loom.Envelope) error{}
	}
	b.handlers[group] = h
	return nil
}

func (b *handBus) envelope(eventType string) *loom.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, env := range b.published {
		if env.Type == eventType {
			return env
		}
	}
	return nil
}

// TestASupersededConsumerHandsTheEventBack: on a transport every
// instance of a group shares, an event may be delivered to an older
// build's instance. It hands it back — an event it subscribes to, and
// one it does not, which it would otherwise have acked as not its own —
// so the transport redelivers and the newest build takes it.
func TestASupersededConsumerHandsTheEventBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	olderBus, newerBus := &handBus{}, &handBus{}
	migrator := startProcess(t, ctx, 0, loom.NewMemoryBus(), false)
	if err := migrator.orders.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := migrator.billing.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	older := startProcess(t, ctx, 10, olderBus, true)
	newer := startProcess(t, ctx, 20, newerBus, true)
	waitFor(t, ctx, "the old build to stand down", func() bool {
		st, err := older.billing.Status(ctx)
		return err == nil && !st.HereNewest
	})

	// the newest build's relay publishes the order's event
	order := placeAcmeOrder(t, ctx, newer.orders)
	waitFor(t, ctx, "the event to be published", func() bool { return newerBus.envelope("OrderPlaced") != nil })
	if olderBus.envelope("OrderPlaced") != nil {
		t.Error("the old build's relay published the event")
	}
	env := newerBus.envelope("OrderPlaced")

	const group = "billing.raiseOnOrder"
	deliver := func(bus *handBus, env *loom.Envelope) error {
		bus.mu.Lock()
		h := bus.handlers[group]
		bus.mu.Unlock()
		if h == nil {
			t.Fatalf("no consumer subscribed as %s", group)
		}
		return h(ctx, env)
	}
	// delivered to the old build: handed back, nothing handled
	if err := deliver(olderBus, env); !errors.Is(err, loom.ErrSuperseded) {
		t.Errorf("the old build's consumer answered %v; want ErrSuperseded", err)
	}
	// and one it does not subscribe to: handed back too, not acked
	foreign := *env
	foreign.Type = "SomethingOnlyTheNewBuildConsumes"
	if err := deliver(olderBus, &foreign); !errors.Is(err, loom.ErrSuperseded) {
		t.Errorf("the old build's consumer answered %v to a type it does not know; want ErrSuperseded, not an ack", err)
	}
	if older.raise.handled.Load() != 0 {
		t.Errorf("the old build handled %d events", older.raise.handled.Load())
	}
	// redelivered to the newest build: handled
	if err := deliver(newerBus, env); err != nil {
		t.Fatalf("the new build's consumer: %v", err)
	}
	if _, version, err := newer.billing.Load(ctx, "Invoice", "acme", order); err != nil || version == 0 || newer.raise.handled.Load() != 1 {
		t.Errorf("the new build: invoice v%d (%v), handled %d", version, err, newer.raise.handled.Load())
	}
	// the newest build still acks what is not its own
	if err := deliver(newerBus, &foreign); err != nil {
		t.Errorf("the new build's consumer on a type it does not subscribe to: %v", err)
	}
}

// TestPubSubRedeliversWhatAnOlderBuildHandsBack: the same on Pub/Sub,
// where both builds' instances share one subscription. Twelve orders
// are placed with an older and a newer build consuming: every one is
// invoiced, all by the newer build, none by the older, and none is
// parked — the older build's nacks come back to the newer one.
func TestPubSubRedeliversWhatAnOlderBuildHandsBack(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("PUBSUB_EMULATOR_HOST not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	topic := "loom-e2e-" + uuid.NewString()[:8]
	newBus := func() loom.Bus {
		b, err := gpub.New(ctx, gpub.Config{ProjectID: "loom-e2e", TopicID: topic})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	migrator := startProcess(t, ctx, 0, loom.NewMemoryBus(), false)
	if err := migrator.orders.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := migrator.billing.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	older := startProcess(t, ctx, 10, newBus(), true)
	newer := startProcess(t, ctx, 20, newBus(), true)
	waitFor(t, ctx, "the old build to stand down", func() bool {
		st, err := older.billing.Status(ctx)
		return err == nil && !st.HereNewest
	})

	const n = 12
	var placed []uuid.UUID
	for i := 0; i < n; i++ {
		placed = append(placed, placeAcmeOrder(t, ctx, newer.orders))
	}
	waitFor(t, ctx, "every order to be invoiced", func() bool {
		for _, id := range placed {
			if _, version, err := migrator.billing.Load(ctx, "Invoice", "acme", id); err != nil || version == 0 {
				return false
			}
		}
		return true
	})
	if got := older.raise.handled.Load(); got != 0 {
		t.Errorf("the old build handled %d events beside a newer one", got)
	}
	if got := newer.raise.handled.Load(); got < n {
		t.Errorf("the new build handled %d events; want all %d", got, n)
	}
	dead, err := migrator.billing.DeadLetters(ctx, 10)
	if err != nil || len(dead) != 0 {
		t.Errorf("dead letters: %+v, %v; want none", dead, err)
	}
}
