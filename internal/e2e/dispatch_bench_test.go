package e2e_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/loomtest"
)

// BenchmarkDispatchConcurrent measures appends per second with many
// dispatches committing at once, each to its own stream: the case the
// service-wide log lock and the append's notification serialise.
// Run with -cpu to set how many dispatch at once, e.g.
// go test ./internal/e2e -run x -bench DispatchConcurrent -cpu 1,8,32.
func BenchmarkDispatchConcurrent(b *testing.B) {
	ctx := context.Background()
	pool := loomtest.Postgres(b)
	keys, err := loom.LocalKeys(bytes.Repeat([]byte("k"), 32))
	if err != nil {
		b.Fatal(err)
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: keys})
	if err != nil {
		b.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := cli.Dispatch(ctx, &billinggen.RaiseInvoice{
				CommandBase: loom.CommandBase{AggregateID: uuid.New(), Namespace: "bench"},
				CustomerId:  uuid.New(),
				AmountCents: 100,
				Currency:    "USD",
			}); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "appends/s")
}
