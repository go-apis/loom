package billing

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// NewRegistry wires implementations into the generated registry. Yours
// to edit.
func NewRegistry() *loom.Registry {
	impl := loomgen.Impl{}
	impl.PostLedger(&PostLedger{})
	impl.RaiseOnOrder(&RaiseOnOrder{})
	impl.CaptureOnPaid(&CaptureOnPaid{})
	impl.Invoice(&Invoice{})
	impl.Payee(&Payee{})
	impl.Contact(&Contact{})
	impl.LedgerEntry(&LedgerEntry{})
	return loomgen.NewRegistry(impl)
}
