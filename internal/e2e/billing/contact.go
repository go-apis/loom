package billing

import (
	"context"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// Contact implements loomgen.ContactHandlers. Yours to edit — loom
// generate never rewrites this file.
type Contact struct{}

func (h *Contact) RecordContact(ctx context.Context, state *loomgen.Contact, cmd *loomgen.RecordContact) ([]loom.DomainEvent, error) {
	return []loom.DomainEvent{&loomgen.ContactRecorded{Email: cmd.Email}}, nil
}
