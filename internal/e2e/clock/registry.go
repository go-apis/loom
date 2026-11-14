package clock

import (
	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// NewRegistry wires implementations into the generated registry. Yours
// to edit.
func NewRegistry() *loom.Registry {
	impl := loomgen.Impl{CoolAnvil: &CoolAnvil{}, Impatient: &Impatient{}}
	impl.Bell(&Bell{})
	impl.Anvil(&Anvil{})
	return loomgen.NewRegistry(impl)
}
