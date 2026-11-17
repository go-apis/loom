package clock

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Loaf implements loomgen.LoafHandlers. Yours to edit.
type Loaf struct{}

func (h *Loaf) BakeLoaf(ctx context.Context, state *loomgen.Loaf, cmd *loomgen.BakeLoaf) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.LoafBaking{OvenId: cmd.OvenId}}, nil
}

func (h *Loaf) FinishLoaf(ctx context.Context, state *loomgen.Loaf, cmd *loomgen.FinishLoaf) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.LoafFinished{Status: "finished"}}, nil
}
