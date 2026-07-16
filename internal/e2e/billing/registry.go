package billing

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// NewRegistry wires implementations into the generated registry. Yours
// to edit.
func NewRegistry() *loom.Registry {
	return loomgen.NewRegistry(loomgen.Impl{
		Invoice:      &Invoice{},
		RaiseOnOrder: &RaiseOnOrder{},
	})
}
