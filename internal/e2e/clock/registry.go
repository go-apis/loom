package clock

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// NewRegistry wires implementations into the generated registry. Yours
// to edit.
func NewRegistry() *loom.Registry {
	impl := loomgen.Impl{}
	impl.Bell(&Bell{})
	return loomgen.NewRegistry(impl)
}
