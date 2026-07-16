package billing

import (
	"context"
	"time"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// LedgerEntry implements loomgen.LedgerEntryHandlers (a record: mutate
// state, optionally announce events). Yours to edit.
type LedgerEntry struct{}

func (h *LedgerEntry) PostLedgerEntry(ctx context.Context, state *loomgen.LedgerEntry, cmd *loomgen.PostLedgerEntry) ([]loom.DomainEvent, error) {
	if !state.PostedAt.IsZero() {
		return nil, nil // already posted: converge
	}
	state.CustomerId = cmd.CustomerId
	state.AmountCents = cmd.AmountCents
	state.Currency = cmd.Currency
	state.PostedAt = time.Now().UTC()
	return []loom.DomainEvent{&loomgen.LedgerEntryPosted{
		AmountCents: cmd.AmountCents,
		Currency:    cmd.Currency,
	}}, nil
}
