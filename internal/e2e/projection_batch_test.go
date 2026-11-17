package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// TestAProjectionFoldsABacklogInOrder: a backlog where each row has
// several events (each invoice raised, then paid) is folded in one batch
// per step; every row ends as its last event left it.
func TestAProjectionFoldsABacklogInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	invoices := make([]uuid.UUID, 50)
	for i := range invoices {
		invoices[i] = uuid.New()
		payInvoice(t, ctx, cli, invoices[i]) // raised, then paid: two events a row
	}
	if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	for _, id := range invoices {
		id := id
		waitFor(t, ctx, "invoice "+id.String()+" folded as paid", func() bool {
			row, err := cli.Entity(ctx, "InvoiceSummary", "default", id)
			if err != nil {
				t.Fatal(err)
			}
			return row != nil && row.(*billinggen.InvoiceSummary).Status == "paid"
		})
	}
}
