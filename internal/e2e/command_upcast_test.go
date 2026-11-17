package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// The orders fixture's CancelOrder is @v(2): v1 called its reason `why`,
// and `upcast CancelOrder @from(1)` renames it. These tests store v1
// CancelOrders as the old binary did — with no command_version, the column
// a pre-version row never had, so it reads as 1 — and as a v1 binary
// would have stamped them.

func ordersClient(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reg *loom.Registry) *loom.Client {
	t.Helper()
	cli, err := loom.New(loom.Config{DB: pool, Registry: reg, Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	return cli
}

func cancelReason(t *testing.T, ctx context.Context, cli *loom.Client, id uuid.UUID) string {
	t.Helper()
	state, _, err := cli.Load(ctx, "Order", "default", id)
	if err != nil {
		t.Fatal(err)
	}
	o := state.(*ordersgen.Order)
	if o.Status != ordersgen.OrderStatusCancelled {
		return ""
	}
	return o.Reason
}

// TestStoredCommandUpcasts: a timer and a batch item holding v1 of a
// command fire after the command changed shape, and the handler gets the
// upcast shape; what loom stores now carries the current version.
func TestStoredCommandUpcasts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	cli := ordersClient(t, ctx, pool, orders.NewRegistry())
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	timed, batched := uuid.New(), uuid.New()
	placeOrder(t, ctx, cli, timed, uuid.New(), 100)
	placeOrder(t, ctx, cli, batched, uuid.New(), 100)

	// a timer row from before command_version existed: due now, v1 shape
	mustExec(t, ctx, pool, `
		INSERT INTO loom_timers (service, namespace, key, command_type, command, meta, fire_at)
		VALUES ('orders','default','legacy-cancel','CancelOrder',
		        jsonb_build_object('aggregate_id',$1::text,'namespace','default','why','customer called'),'{}', now())`, timed)
	// a batch item a v1 binary stamped as v1
	batchID := uuid.New()
	mustExec(t, ctx, pool, `
		INSERT INTO loom_batches (service, namespace, id, status, total) VALUES ('orders','default',$1,'running',1)`, batchID)
	mustExec(t, ctx, pool, `
		INSERT INTO loom_batch_items (service, batch_id, seq, command_type, command, command_version)
		VALUES ('orders',$1,0,'CancelOrder',
		        jsonb_build_object('aggregate_id',$2::text,'namespace','default','why','out of stock'), 1)`, batchID, batched)

	// the hop is declared, so the stored v1 rows do not stop a migration
	if err := cli.Migrate(ctx); err != nil {
		t.Fatalf("migrate with v1 rows and an upcast declared: %v", err)
	}

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the v1 timer fires upcast", func() bool { return cancelReason(t, ctx, cli, timed) != "" })
	if got := cancelReason(t, ctx, cli, timed); got != "customer called" {
		t.Fatalf("timer: handler saw reason %q, want the upcast v1 `why`", got)
	}
	waitFor(t, ctx, "the v1 batch item runs upcast", func() bool { return cancelReason(t, ctx, cli, batched) != "" })
	if got := cancelReason(t, ctx, cli, batched); got != "out of stock" {
		t.Fatalf("batch item: handler saw reason %q, want the upcast v1 `why`", got)
	}

	// a plan held across the deploy: made now (stamped v2), rewritten as
	// a v1 binary would have stored it
	planned := uuid.New()
	placeOrder(t, ctx, cli, planned, uuid.New(), 100)
	plan, err := cli.Plan(ctx, &ordersgen.CancelOrder{CommandBase: loom.CommandBase{AggregateID: planned, Namespace: "default"}}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var v int
	if err := pool.QueryRow(ctx, `SELECT command_version FROM loom_plans WHERE id=$1`, plan.ID).Scan(&v); err != nil || v != 2 {
		t.Fatalf("a fresh plan is stored as v%d (%v), want 2", v, err)
	}
	mustExec(t, ctx, pool, `
		UPDATE loom_plans SET command_version=1,
		       command=jsonb_build_object('aggregate_id',$2::text,'namespace','default','why','duplicate')
		WHERE id=$1`, plan.ID, planned)
	if err := cli.RunPlan(ctx, "default", plan.ID); err != nil {
		t.Fatal(err)
	}
	if got := cancelReason(t, ctx, cli, planned); got != "duplicate" {
		t.Fatalf("plan: handler saw reason %q, want the upcast v1 `why`", got)
	}

	// a timer parked before parking kept the version: its envelope has
	// no command_version, so it reads as 1 when redriven
	parked := uuid.New()
	placeOrder(t, ctx, cli, parked, uuid.New(), 100)
	var dl int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO loom_dead_letters (service, runner, envelope, error, attempts)
		VALUES ('orders','timer', jsonb_build_object('timer_key','parked','command_type','CancelOrder',
		        'command', jsonb_build_object('aggregate_id',$1::text,'namespace','default','why','fraud')), 'x', 3)
		RETURNING id`, parked).Scan(&dl); err != nil {
		t.Fatal(err)
	}
	if err := cli.RedriveDeadLetter(ctx, dl); err != nil {
		t.Fatal(err)
	}
	if got := cancelReason(t, ctx, cli, parked); got != "fraud" {
		t.Fatalf("redriven timer: handler saw reason %q, want the upcast v1 `why`", got)
	}

	// what loom stores now is stamped with the command's current @v
	fresh := uuid.New()
	if err := cli.Schedule(ctx, loom.After(&ordersgen.CancelOrder{CommandBase: loom.CommandBase{AggregateID: fresh, Namespace: "default"}}, time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT command_version FROM loom_timers WHERE command_type='CancelOrder' AND command->>'aggregate_id'=$1`, fresh.String()).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 2 {
		t.Fatalf("a fresh CancelOrder timer is stored as v%d, want 2", v)
	}
	id, err := cli.EnqueueBatch(ctx, "default", []loom.Command{&ordersgen.CancelOrder{CommandBase: loom.CommandBase{AggregateID: uuid.New(), Namespace: "default"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT command_version FROM loom_batch_items WHERE batch_id=$1`, id).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 2 {
		t.Fatalf("a fresh CancelOrder batch item is stored as v%d, want 2", v)
	}
}

// TestMigrateRefusesStrandedStoredCommands: the same change of shape with
// no upcast declared — CancelOrder @v(2) and nothing to lift v1 — and
// Migrate refuses, naming the command, the version and the stored rows.
func TestMigrateRefusesStrandedStoredCommands(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	if err := ordersClient(t, ctx, pool, orders.NewRegistry()).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// two v1 rows (one from before the column, one stamped) and a v2 row
	for i, sql := range []string{
		`INSERT INTO loom_timers (service, namespace, key, command_type, command, meta, fire_at)
		 VALUES ('orders','default','old-1','CancelOrder','{"why":"x"}','{}', now() + interval '3 days')`,
		`INSERT INTO loom_timers (service, namespace, key, command_type, command, meta, fire_at, command_version)
		 VALUES ('orders','default','old-2','CancelOrder','{"why":"y"}','{}', now() + interval '3 days', 1)`,
		`INSERT INTO loom_timers (service, namespace, key, command_type, command, meta, fire_at, command_version)
		 VALUES ('orders','default','new-1','CancelOrder','{"reason":"z"}','{}', now() + interval '3 days', 2)`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}

	reg := orders.NewRegistry()
	for _, agg := range reg.Aggregates {
		for _, def := range agg.Commands {
			if def.Name == "CancelOrder" {
				def.Upcasts = nil // the schema without `upcast CancelOrder @from(1)`
			}
		}
	}
	err := ordersClient(t, ctx, pool, reg).Migrate(ctx)
	if err == nil {
		t.Fatal("migrate ran with v1 CancelOrder timers stored and no upcast to lift them")
	}
	for _, want := range []string{"CancelOrder v1", "2 stored rows", "@from(1)"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not say %q", err, want)
		}
	}

	// once the rows are gone (fired, cancelled), the same build migrates
	mustExec(t, ctx, pool, `DELETE FROM loom_timers WHERE key IN ('old-1','old-2')`)
	if err := ordersClient(t, ctx, pool, reg).Migrate(ctx); err != nil {
		t.Fatalf("migrate with only current rows stored: %v", err)
	}
}
