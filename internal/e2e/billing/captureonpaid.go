package billing

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-apis/loom"

	"github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// Gateway stands in for the external payment provider. Tests script its
// failures to prove the effect journal's call-once discipline.
var Gateway = &FakeGateway{}

type FakeGateway struct {
	mu        sync.Mutex
	Calls     int // capture invocations that reached the "provider"
	FailCalls int // make the next N capture calls fail
}

func (g *FakeGateway) Capture(invoice string, cents int64) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Calls++
	if g.FailCalls > 0 {
		g.FailCalls--
		return "", fmt.Errorf("gateway: capture declined (scripted)")
	}
	return "cap_" + invoice, nil
}

func (g *FakeGateway) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Calls, g.FailCalls = 0, 0
}

// FailReactAfterCapture makes the reaction fail N times after the capture
// call — the retries must replay the journaled receipt, not charge again.
var FailReactAfterCapture = 0

// LastReceipt records what the reaction saw, for assertions.
var LastReceipt string

// CaptureOnPaid implements loomgen.CaptureOnPaidReactions. Yours to edit.
type CaptureOnPaid struct{}

func (h *CaptureOnPaid) OnInvoicePaid(ctx context.Context, evt *loom.Event, data *loomgen.InvoicePaid) ([]loom.Command, error) {
	receipt, err := loom.Once(ctx, "gateway_capture", func(ctx context.Context) (string, error) {
		return Gateway.Capture(evt.AggregateID.String(), data.AmountCents)
	})
	if err != nil {
		return nil, err
	}
	if FailReactAfterCapture > 0 {
		FailReactAfterCapture--
		return nil, fmt.Errorf("scripted post-capture failure")
	}
	LastReceipt = receipt
	return nil, nil
}
