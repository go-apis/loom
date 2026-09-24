package e2e_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// ghostType is an event type no registry declares — what a rolled-back
// deploy, a deleted declaration, or another team's stray write leaves in
// the log. Nothing subscribes to it, so nothing should ever decode it.
const ghostType = "PromotionExpired"

// TestLazyDecodeSkipsUnknownType proves runners decode only what they
// subscribe to: an undeclared row sits in the middle of billing's log and
// neither the invoiceSummary projection nor the captureOnPaid process
// notices — both step over it, checkpoints advance, no error is logged.
// Before lazy decode, readLog decoded every row eagerly and this one row
// failed the whole batch for every runner sharing the fan-out buffer.
func TestLazyDecodeSkipsUnknownType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	errs := &errorLog{}
	cli, err := loom.New(loom.Config{
		DB:       pool,
		Bus:      loom.NewMemoryBus(),
		Registry: billing.NewRegistry(),
		Keys:     testKeys(t),
		Logger:   slog.New(errs),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	ghost := insertGhost(t, ctx, pool)

	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// an event each runner DOES subscribe to, after the ghost: both must
	// read past the undecodable row to reach it
	invoice := uuid.New()
	raiseInvoice(t, ctx, cli, invoice)

	for _, runner := range []string{"projection:invoiceSummary", "process:captureOnPaid"} {
		waitFor(t, ctx, runner+" past the undeclared row", func() bool {
			return checkpointOf(t, ctx, pool, "billing", runner) > ghost
		})
	}
	waitFor(t, ctx, "invoice projected", func() bool {
		e, err := cli.Entity(ctx, "InvoiceSummary", "default", invoice)
		return err == nil && e != nil
	})

	if logged := errs.matching("runner step failed", "log reader failed"); len(logged) > 0 {
		t.Fatalf("a row nobody subscribes to made a runner fail: %v", logged)
	}
}

// TestMigrateRejectsUnknownType is the other half of the same bargain: the
// undeclared type is loud exactly once, at migrate time, and never in the
// runners. Migrate refuses it; runners started separately carry on
// advancing their own checkpoints over it.
func TestMigrateRejectsUnknownType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	newClient := func() *loom.Client {
		t.Helper()
		cli, err := loom.New(loom.Config{
			DB: pool, Bus: loom.NewMemoryBus(),
			Registry: billing.NewRegistry(), Keys: testKeys(t),
		})
		if err != nil {
			t.Fatal(err)
		}
		return cli
	}

	if err := newClient().Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ghost := insertGhost(t, ctx, pool)

	err := newClient().Migrate(ctx)
	if err == nil {
		t.Fatal("Migrate accepted a log holding an undeclared event type")
	}
	if !strings.Contains(err.Error(), ghostType) {
		t.Fatalf("Migrate error does not name the undeclared type: %v", err)
	}

	// the runners are unaffected: a separately started client steps over
	// the same row and keeps projecting
	runners := newClient()
	if err := runners.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	invoice := uuid.New()
	raiseInvoice(t, ctx, runners, invoice)
	waitFor(t, ctx, "invoice projected despite the undeclared row", func() bool {
		e, err := runners.Entity(ctx, "InvoiceSummary", "default", invoice)
		return err == nil && e != nil
	})
	for _, runner := range []string{"projection:invoiceSummary", "process:captureOnPaid"} {
		waitFor(t, ctx, runner+" past the undeclared row", func() bool {
			return checkpointOf(t, ctx, pool, "billing", runner) > ghost
		})
	}
}

// insertGhost writes one row of ghostType into billing's log and returns
// its global_seq.
func insertGhost(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var seq int64
	err := pool.QueryRow(ctx, `
		INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, schema_version, data)
		VALUES ('billing','default','Invoice',$1,1,$2,1,'{"code":"SUMMER"}')
		RETURNING global_seq`, uuid.New(), ghostType).Scan(&seq)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func raiseInvoice(t *testing.T, ctx context.Context, cli *loom.Client, id uuid.UUID) {
	t.Helper()
	err := cli.Dispatch(ctx, &billinggen.RaiseInvoice{
		CommandBase: loom.CommandBase{AggregateID: id, Namespace: "default"},
		CustomerId:  uuid.New(),
		AmountCents: 1200,
		Currency:    "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func checkpointOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, service, runner string) int64 {
	t.Helper()
	var seq int64
	err := pool.QueryRow(ctx, `
		SELECT global_seq FROM loom_checkpoints WHERE service=$1 AND runner=$2`,
		service, runner).Scan(&seq)
	if err != nil {
		return -1 // not checkpointed yet
	}
	return seq
}

// errorLog is a slog handler that keeps ERROR messages, so a test can
// assert a runner said nothing.
type errorLog struct {
	mu   sync.Mutex
	msgs []string
}

func (h *errorLog) Enabled(context.Context, slog.Level) bool { return true }

func (h *errorLog) Handle(_ context.Context, r slog.Record) error {
	if r.Level < slog.LevelError {
		return nil
	}
	line := r.Message
	r.Attrs(func(a slog.Attr) bool {
		line += " " + a.String()
		return true
	})
	h.mu.Lock()
	h.msgs = append(h.msgs, line)
	h.mu.Unlock()
	return nil
}

func (h *errorLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *errorLog) WithGroup(string) slog.Handler      { return h }

func (h *errorLog) matching(needles ...string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, m := range h.msgs {
		for _, n := range needles {
			if strings.Contains(m, n) {
				out = append(out, m)
				break
			}
		}
	}
	return out
}
