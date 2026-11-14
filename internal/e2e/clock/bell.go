package clock

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Bell implements loomgen.BellHandlers. Yours to edit — loom generate never
// rewrites this file.
type Bell struct{}

// Refuse scripts the next N rings to be refused by the handler: a tick
// that fails, to prove the series goes on.
var Refuse atomic.Int64

// Refused counts the rings the handler refused.
var Refused atomic.Int64

func (h *Bell) RingBell(ctx context.Context, state *loomgen.Bell, cmd *loomgen.RingBell) ([]loom.DomainEvent, error) {
	if Refuse.Load() > 0 {
		Refuse.Add(-1)
		Refused.Add(1)
		return nil, fmt.Errorf("the bell is stuck (scripted)")
	}
	return []loom.DomainEvent{&loomgen.BellRang{Rings: state.Rings + 1}}, nil
}
