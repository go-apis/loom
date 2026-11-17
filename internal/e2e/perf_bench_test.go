package e2e_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	"github.com/go-apis/loom/loomtest"
)

// The hot paths a loom service spends its time on, against a real
// Postgres: one dispatch at a time, a load of a long stream, a read of a
// projection row, and projections catching up with a backlog. Run with
//
//	LOOM_TEST_PG=... go test ./internal/e2e -run '^$' -bench 'Perf' -benchtime 3s -count 3

func benchClient(b *testing.B, reg *loom.Registry) *loom.Client {
	b.Helper()
	pool := loomtest.Postgres(b)
	keys, err := loom.LocalKeys(bytes.Repeat([]byte("k"), 32))
	if err != nil {
		b.Fatal(err)
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: reg, Keys: keys, Generation: 1, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		b.Fatal(err)
	}
	if err := cli.Migrate(context.Background()); err != nil {
		b.Fatal(err)
	}
	return cli
}

// BenchmarkPerfDispatchOne: one command, one new stream, one at a time.
func BenchmarkPerfDispatchOne(b *testing.B) {
	ctx := context.Background()
	cli := benchClient(b, billing.NewRegistry())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cli.Dispatch(ctx, &billinggen.RaiseInvoice{
			CommandBase: loom.CommandBase{AggregateID: uuid.New(), Namespace: "bench"},
			CustomerId:  uuid.New(), AmountCents: 100, Currency: "USD",
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerfDispatchToALongStream: a command on a stream of 500
// events with no snapshot: the handler's state is folded from them all.
func BenchmarkPerfDispatchToALongStream(b *testing.B) {
	ctx := context.Background()
	cli := benchClient(b, clock.NewRegistry())
	kiln := loom.CommandBase{Namespace: "bench", AggregateID: uuid.New()}
	for i := 0; i < 500; i++ {
		if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerfLoadALongStream: Load of a 500-event stream.
func BenchmarkPerfLoadALongStream(b *testing.B) {
	ctx := context.Background()
	cli := benchClient(b, clock.NewRegistry())
	kiln := loom.CommandBase{Namespace: "bench", AggregateID: uuid.New()}
	for i := 0; i < 500; i++ {
		if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, v, err := cli.Load(ctx, "Kiln", kiln.Namespace, kiln.AggregateID); err != nil || v != 500 {
			b.Fatalf("load: version %d, %v", v, err)
		}
	}
}

// BenchmarkPerfEntityRead: a projection row read.
func BenchmarkPerfEntityRead(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := benchClient(b, billing.NewRegistry())
	if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
		b.Fatal(err)
	}
	invoice := uuid.New()
	if err := cli.Dispatch(ctx, &billinggen.RaiseInvoice{
		CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "bench"},
		CustomerId:  uuid.New(), AmountCents: 100, Currency: "USD",
	}); err != nil {
		b.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		if row, _ := cli.Entity(ctx, "InvoiceSummary", "bench", invoice); row != nil {
			break
		}
		if time.Now().After(deadline) {
			b.Fatal("the projection never folded the invoice")
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if row, err := cli.Entity(ctx, "InvoiceSummary", "bench", invoice); err != nil || row == nil {
			b.Fatalf("read: %v", err)
		}
	}
}

// BenchmarkPerfProjectionCatchUp: a backlog of 2000 events folded by a
// freshly started service's projections; reports events folded per
// second. Run with -benchtime 1x.
func BenchmarkPerfProjectionCatchUp(b *testing.B) {
	const backlog = 2000
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ctx, cancel := context.WithCancel(context.Background())
		cli := benchClient(b, billing.NewRegistry())
		ids := make([]uuid.UUID, backlog)
		for j := range ids {
			ids[j] = uuid.New()
			if err := cli.Dispatch(ctx, &billinggen.RaiseInvoice{
				CommandBase: loom.CommandBase{AggregateID: ids[j], Namespace: "bench"},
				CustomerId:  uuid.New(), AmountCents: 100, Currency: "USD",
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		began := time.Now()
		if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
			b.Fatal(err)
		}
		last := ids[backlog-1]
		for {
			if row, _ := cli.Entity(ctx, "InvoiceSummary", "bench", last); row != nil {
				break
			}
			if time.Since(began) > 2*time.Minute {
				b.Fatal("the projection never caught up")
			}
			time.Sleep(5 * time.Millisecond)
		}
		b.ReportMetric(float64(backlog)/time.Since(began).Seconds(), "events/s")
		b.StopTimer()
		cancel()
	}
}
