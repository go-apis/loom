package clock

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Oven implements loomgen.OvenHandlers. Yours to edit.
type Oven struct{}

func (h *Oven) HeatOven(ctx context.Context, state *loomgen.Oven, cmd *loomgen.HeatOven) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.OvenHeated{Status: "heating"}}, nil
}

func (h *Oven) RestOven(ctx context.Context, state *loomgen.Oven, cmd *loomgen.RestOven) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.OvenRested{Status: "resting"}}, nil
}
