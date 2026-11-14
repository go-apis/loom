package orders

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// Parcel implements loomgen.ParcelHandlers. Yours to edit — loom generate never
// rewrites this file.
type Parcel struct{}

func (h *Parcel) RegisterParcel(ctx context.Context, state *loomgen.Parcel, cmd *loomgen.RegisterParcel) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.ParcelRegistered{DepotId: cmd.DepotId, Label: cmd.Label}}, nil
}

func (h *Parcel) RelabelParcel(ctx context.Context, state *loomgen.Parcel, cmd *loomgen.RelabelParcel) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.ParcelRelabelled{Label: cmd.Label}}, nil
}
