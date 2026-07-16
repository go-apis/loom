package orders

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// NewRegistry wires implementations into the generated registry. Yours
// to edit.
func NewRegistry() *loom.Registry {
	return loomgen.NewRegistry(loomgen.Impl{
		Order:              &Order{},
		ShipOnPayment:      &ShipOnPayment{},
		ScheduleAutoCancel: &ScheduleAutoCancel{},
		DropAutoCancel:     &DropAutoCancel{},
	})
}
