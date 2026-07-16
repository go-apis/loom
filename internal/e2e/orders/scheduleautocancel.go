package orders

import (
	"context"
	"time"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// AutoCancelAfter is how long an order may sit unpaid before it cancels
// itself. Variable so tests can shrink it.
var AutoCancelAfter = 5 * time.Second

// ScheduleAutoCancel implements loomgen.ScheduleAutoCancelReactions. Yours to edit.
type ScheduleAutoCancel struct{}

func (h *ScheduleAutoCancel) OnOrderPlaced(ctx context.Context, evt *loom.Event, data *loomgen.OrderPlaced) ([]loom.Command, error) {
	return []loom.Command{
		loom.After(&loomgen.CancelOrder{
			CommandBase: loom.CommandBase{AggregateID: evt.AggregateID, Namespace: evt.Namespace},
		}, AutoCancelAfter),
	}, nil
}
