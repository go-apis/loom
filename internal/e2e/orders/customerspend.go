package orders

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// CustomerSpend implements loomgen.CustomerSpendFolds — the @fold projection's hand-written
// fold. Runs on the checkpointed projection runner; rebuild refolds the
// whole log, so keep it deterministic. Yours to edit.
type CustomerSpend struct{}

func (f *CustomerSpend) OnOrderPlaced(state *loomgen.CustomerSpend, evt *loom.Event, data *loomgen.OrderPlaced) error {
	state.OrderCount++
	state.SpendCents += data.TotalCents
	return nil
}
