package clock

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Anvil implements loomgen.AnvilHandlers. Yours to edit — loom generate never
// rewrites this file.
type Anvil struct{}

func (h *Anvil) RecordAnvilCool(ctx context.Context, state *loomgen.Anvil, cmd *loomgen.RecordAnvilCool) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.AnvilCooled{Cool: true}}, nil
}

func (h *Anvil) StrikeAnvil(ctx context.Context, state *loomgen.Anvil, cmd *loomgen.StrikeAnvil) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.AnvilStruck{Cool: false}}, nil
}
