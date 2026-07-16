package orders

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// DropAutoCancel implements loomgen.DropAutoCancelReactions. Yours to edit.
type DropAutoCancel struct{}

func (h *DropAutoCancel) OnOrderShipped(ctx context.Context, evt *loom.Event, data *loomgen.OrderShipped) ([]loom.Command, error) {
	// same default key as the schedule (command type + target), so this
	// deletes the pending auto-cancel
	return []loom.Command{
		loom.CancelTimer(&loomgen.CancelOrder{
			CommandBase: loom.CommandBase{AggregateID: evt.AggregateID, Namespace: evt.Namespace},
		}),
	}, nil
}
