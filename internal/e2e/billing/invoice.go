package billing

import (
	"context"
	"fmt"
	"time"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// Invoice implements loomgen.InvoiceHandlers. Yours to edit — loom generate
// never rewrites this file.
type Invoice struct{}

func (h *Invoice) RaiseInvoice(ctx context.Context, state *loomgen.Invoice, cmd *loomgen.RaiseInvoice) ([]loom.DomainEvent, error) {
	if state.Status != "" {
		return nil, nil // bus redelivery after the invoice exists: converge
	}
	return []loom.DomainEvent{&loomgen.InvoiceRaised{
		Status:      "raised",
		CustomerId:  cmd.CustomerId,
		AmountCents: cmd.AmountCents,
		Currency:    cmd.Currency,
	}}, nil
}

func (h *Invoice) MarkInvoicePaid(ctx context.Context, state *loomgen.Invoice, cmd *loomgen.MarkInvoicePaid) ([]loom.DomainEvent, error) {
	if state.Status != "raised" {
		return nil, fmt.Errorf("cannot pay invoice in status %q", state.Status)
	}
	return []loom.DomainEvent{&loomgen.InvoicePaid{
		Status: "paid",
		PaidAt: time.Now().UTC(),
	}}, nil
}
