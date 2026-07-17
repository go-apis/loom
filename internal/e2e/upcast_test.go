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
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestUpcasts proves stored-event payload migration: OrderCancelled is
// @v(2) (added `reason`) with an upcast from v1. A hand-inserted v1 row —
// exactly what a pre-migration deploy would have written — lifts through
// both decode paths (aggregate replay and projection catch-up), and a
// stored version NEWER than the registry is a loud error, never a
// zero-value fold.
func TestUpcasts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// a v1-era stream: OrderPlaced (current shape) then OrderCancelled v1
	// with no `reason` field, as the old binary wrote it
	legacy := uuid.New()
	mustExec(t, ctx, pool, `
		INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, schema_version, data)
		VALUES ('orders','default','Order',$1,1,'OrderPlaced',1,
		        '{"status":"placed","customer_id":"`+uuid.NewString()+`","items":[],"total_cents":700,"currency":"USD"}'),
		       ('orders','default','Order',$1,2,'OrderCancelled',1,'{"status":"cancelled"}')`, legacy)

	// aggregate replay decodes through the upcast chain
	state, version, err := cli.Load(ctx, "Order", "default", legacy)
	if err != nil {
		t.Fatal(err)
	}
	order := state.(*ordersgen.Order)
	if version != 2 || order.Status != "cancelled" || order.Reason != "unspecified" {
		t.Fatalf("v1 row did not upcast: version=%d status=%q reason=%q", version, order.Status, order.Reason)
	}

	// projection catch-up reads the same chain: the summary row folds the
	// lifted payload (and reason rides the @table typed column)
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "legacy row projected", func() bool {
		e, err := cli.Entity(ctx, "OrderSummary", "default", legacy)
		if err != nil || e == nil {
			return false
		}
		s := e.(*ordersgen.OrderSummary)
		return s.Status == "cancelled" && s.Reason == "unspecified"
	})

	// a freshly emitted OrderCancelled carries v2 and its own reason
	fresh := uuid.New()
	placeOrder(t, ctx, cli, fresh, uuid.New(), 500)
	if err := cli.Dispatch(ctx, &ordersgen.CancelOrder{CommandBase: loom.CommandBase{AggregateID: fresh, Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	state, _, err = cli.Load(ctx, "Order", "default", fresh)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.(*ordersgen.Order).Reason; got != "requested" {
		t.Fatalf("fresh cancel reason: %q", got)
	}
	var sv int
	if err := pool.QueryRow(ctx, `
		SELECT schema_version FROM loom_events WHERE aggregate_id=$1 AND type='OrderCancelled'`, fresh).Scan(&sv); err != nil {
		t.Fatal(err)
	}
	if sv != 2 {
		t.Fatalf("fresh OrderCancelled stored as v%d", sv)
	}

	// deploy skew: a stored version this registry doesn't know is loud
	skewed := uuid.New()
	mustExec(t, ctx, pool, `
		INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, schema_version, data)
		VALUES ('orders','default','Order',$1,1,'OrderCancelled',3,'{"status":"cancelled","reason":"future"}')`, skewed)
	_, _, err = cli.Load(ctx, "Order", "default", skewed)
	var uce *loom.UpcastError
	if !errors.As(err, &uce) || !strings.Contains(err.Error(), "deploy skew") {
		t.Fatalf("expected a deploy-skew UpcastError, got %v", err)
	}
}

func mustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}
