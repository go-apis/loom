package clock

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// Ticket implements loomgen.TicketHandlers. Yours to edit — loom generate never
// rewrites this file.
type Ticket struct{}

func (h *Ticket) CloseTicket(ctx context.Context, state *loomgen.Ticket, cmd *loomgen.CloseTicket) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.TicketClosed{Status: "closed", Verdict: cmd.Verdict}}, nil
}

func (h *Ticket) OpenTicket(ctx context.Context, state *loomgen.Ticket, cmd *loomgen.OpenTicket) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.TicketOpened{Status: "open"}}, nil
}
