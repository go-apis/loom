package clock

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Anvil implements loomgen.AnvilHandlers. Yours to edit — loom generate never
// rewrites this file.
type Anvil struct{}

// Brittle scripts the anvil to refuse being recorded as cool.
var Brittle atomic.Bool

func (h *Anvil) RecordAnvilCool(ctx context.Context, state *loomgen.Anvil, cmd *loomgen.RecordAnvilCool) ([]loom.DomainEvent, error) {
	if Brittle.Load() {
		return nil, errors.New("the anvil cracked")
	}
	return []loom.DomainEvent{&loomgen.AnvilCooled{Cool: true}}, nil
}

func (h *Anvil) StrikeAnvil(ctx context.Context, state *loomgen.Anvil, cmd *loomgen.StrikeAnvil) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.AnvilStruck{Cool: false}}, nil
}
