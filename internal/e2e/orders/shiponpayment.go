package orders

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// ShipOnPayment implements loomgen.ShipOnPaymentReactions. Yours to edit.
type ShipOnPayment struct{}

func (h *ShipOnPayment) OnInvoicePaid(ctx context.Context, evt *loom.Event, data *loomgen.InvoicePaid) ([]loom.Command, error) {
	// the invoice shares the order's aggregate id
	return []loom.Command{&loomgen.ShipOrder{
		CommandBase: loom.CommandBase{AggregateID: evt.AggregateID, Namespace: evt.Namespace},
	}}, nil
}
