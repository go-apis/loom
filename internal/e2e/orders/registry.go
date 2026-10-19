package orders

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// NewRegistry wires implementations into the generated registry. Yours
// to edit.
func NewRegistry() *loom.Registry {
	impl := loomgen.Impl{
		ShipOnPayment:      &ShipOnPayment{},
		ScheduleAutoCancel: &ScheduleAutoCancel{},
		DropAutoCancel:     &DropAutoCancel{},
		CustomerSpend:      &CustomerSpend{},
		Upcasts:            &Upcasts{},
	}
	impl.Order(&Order{})
	return loomgen.NewRegistry(impl)
}
