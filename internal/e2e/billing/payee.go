package billing

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// Payee implements loomgen.PayeeHandlers. Yours to edit — loom generate never
// rewrites this file.
type Payee struct{}

func (h *Payee) RegisterPayee(ctx context.Context, state *loomgen.Payee, cmd *loomgen.RegisterPayee) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.PayeeRegistered{
		Name:     cmd.Name,
		Tin:      cmd.Tin,
		TinLast4: cmd.TinLast4,
	}}, nil
}
