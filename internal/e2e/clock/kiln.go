package clock

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Kiln implements loomgen.KilnHandlers. Yours to edit — loom generate never
// rewrites this file.
type Kiln struct{}

func (h *Kiln) LightKiln(ctx context.Context, state *loomgen.Kiln, cmd *loomgen.LightKiln) ([]loom.DomainEvent, error) {
	if state.Lit {
		return nil, nil
	}
	return []loom.DomainEvent{&loomgen.KilnLit{Lit: true}}, nil
}

func (h *Kiln) StokeKiln(ctx context.Context, state *loomgen.Kiln, cmd *loomgen.StokeKiln) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.KilnStoked{Stokes: state.Stokes + 1}}, nil
}

func (h *Kiln) RecordKilnFired(ctx context.Context, state *loomgen.Kiln, cmd *loomgen.RecordKilnFired) ([]loom.DomainEvent, error) {
	if state.Fired {
		return nil, nil
	}
	return []loom.DomainEvent{&loomgen.KilnFired{Fired: true}}, nil
}
