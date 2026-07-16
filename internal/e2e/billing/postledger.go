package billing

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// PostLedger implements loomgen.PostLedgerReactions. Yours to edit.
type PostLedger struct{}

func (h *PostLedger) OnInvoicePaid(ctx context.Context, evt *loom.Event, data *loomgen.InvoicePaid) ([]loom.Command, error) {
	// in-transaction with the payment: the ledger row commits atomically
	// with InvoicePaid
	return []loom.Command{
		&loomgen.PostLedgerEntry{
			CommandBase: loom.CommandBase{AggregateID: evt.AggregateID, Namespace: evt.Namespace},
			CustomerId:  data.CustomerId,
			AmountCents: data.AmountCents,
			Currency:    data.Currency,
		},
	}, nil
}
