package orders

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// NewRegistry wires implementations into the generated registry. Yours
// to edit.
func NewRegistry() *loom.Registry {
	impl := loomgen.Impl{Upcasts: &Upcasts{}, CommandUpcasts: &CommandUpcasts{}}
	impl.ScheduleAutoCancel(&ScheduleAutoCancel{})
	impl.DropAutoCancel(&DropAutoCancel{})
	impl.ShipOnPayment(&ShipOnPayment{})
	impl.CustomerSpend(&CustomerSpend{})
	impl.Order(&Order{})
	impl.Parcel(&Parcel{})
	return loomgen.NewRegistry(impl)
}
