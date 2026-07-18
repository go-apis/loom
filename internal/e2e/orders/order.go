package orders

import (
	"context"
	"fmt"
	"time"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// Order implements loomgen.OrderHandlers. Yours to edit — loom generate never
// rewrites this file.
type Order struct{}

func (h *Order) PlaceOrder(ctx context.Context, state *loomgen.Order, cmd *loomgen.PlaceOrder) ([]loom.DomainEvent, error) {
	if state.Status != "" {
		return nil, fmt.Errorf("order already exists (status %s)", state.Status)
	}
	var total int64
	for _, item := range cmd.Items {
		total += item.PriceCents * item.Quantity
	}
	return []loom.DomainEvent{&loomgen.OrderPlaced{
		Status:     loomgen.OrderStatusPlaced,
		CustomerId: cmd.CustomerId,
		Items:      cmd.Items,
		TotalCents: total,
		Currency:   cmd.Currency,
	}}, nil
}

func (h *Order) ShipOrder(ctx context.Context, state *loomgen.Order, cmd *loomgen.ShipOrder) ([]loom.DomainEvent, error) {
	if state.Status == loomgen.OrderStatusShipped {
		return nil, nil // redelivery after we already shipped: converge
	}
	if state.Status != loomgen.OrderStatusPlaced {
		return nil, fmt.Errorf("cannot ship order in status %q", state.Status)
	}
	return []loom.DomainEvent{&loomgen.OrderShipped{
		Status:    loomgen.OrderStatusShipped,
		ShippedAt: time.Now().UTC(),
	}}, nil
}

func (h *Order) CancelOrder(ctx context.Context, state *loomgen.Order, cmd *loomgen.CancelOrder) ([]loom.DomainEvent, error) {
	if state.Status != loomgen.OrderStatusPlaced {
		return nil, fmt.Errorf("cannot cancel order in status %q", state.Status)
	}
	return []loom.DomainEvent{&loomgen.OrderCancelled{Status: loomgen.OrderStatusCancelled, Reason: "requested"}}, nil
}

func (h *Order) RequestContract(ctx context.Context, state *loomgen.Order, cmd *loomgen.RequestContract) ([]loom.DomainEvent, error) {
	if state.Status == "" {
		return nil, fmt.Errorf("no such order")
	}
	return []loom.DomainEvent{&loomgen.ContractRequested{Requested: cmd.Contract}}, nil
}

func (h *Order) AttachContract(ctx context.Context, state *loomgen.Order, cmd *loomgen.AttachContract) ([]loom.DomainEvent, error) {
	if state.Contract != nil && state.Contract.ID == cmd.Contract.ID {
		return nil, nil // finalize redelivery: converge
	}
	return []loom.DomainEvent{&loomgen.ContractAttached{Contract: cmd.Contract}}, nil
}
