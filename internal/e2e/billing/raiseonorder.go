package billing

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// RaiseOnOrder implements loomgen.RaiseOnOrderReactions. Yours to edit.
type RaiseOnOrder struct{}

func (h *RaiseOnOrder) OnOrderPlaced(ctx context.Context, evt *loom.Event, data *loomgen.OrderPlaced) ([]loom.Command, error) {
	// reuse the order's aggregate id so redeliveries converge on one invoice
	return []loom.Command{&loomgen.RaiseInvoice{
		CommandBase: loom.CommandBase{AggregateID: evt.AggregateID, Namespace: evt.Namespace},
		CustomerId:  data.CustomerId,
		AmountCents: data.TotalCents,
		Currency:    data.Currency,
	}}, nil
}
