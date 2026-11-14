package clock

import (
	"context"
	"sync/atomic"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Impatient implements loomgen.ImpatientReactions. Yours to edit.
type Impatient struct{}

// Impatience scripts the process to answer NotYet, which it has no
// @retry to look again with.
var Impatience atomic.Bool

func (h *Impatient) OnAnvilStruck(ctx context.Context, evt *loom.Event, data *loomgen.AnvilStruck) ([]loom.Command, error) {
	if Impatience.Load() {
		return nil, loom.NotYet("the anvil is still hot")
	}
	return nil, nil
}
