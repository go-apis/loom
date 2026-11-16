package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
)

// TestAnAppendWakesTheOtherInstances: an append on one instance is told
// to another by its notification after commit, not by the other's poll.
// The other instance polls every 20s; its projection folds the event
// within a couple of seconds, so only the notification can have woken it.
func TestAnAppendWakesTheOtherInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	writer, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t), RunnerIdlePoll: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Start(ctx, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	// let the reader's runners settle into their idle wait
	time.Sleep(2 * time.Second)

	for i := 0; i < 3; i++ {
		invoice := uuid.New()
		began := time.Now()
		payInvoice(t, ctx, writer, invoice)
		deadline := time.Now().Add(5 * time.Second)
		for {
			row, err := reader.Entity(ctx, "InvoiceSummary", "default", invoice)
			if err != nil {
				t.Fatal(err)
			}
			if row != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("append %d: the other instance had not folded it after %s; its poll is 20s, so the notification did not wake it", i, time.Since(began))
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}
