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

// CallsN reads the capture count under the lock (tests poll it).
func (g *FakeGateway) CallsN() int { g.mu.Lock(); defer g.mu.Unlock(); return g.Calls }

// SetFailCalls scripts the next N captures to fail.
func (g *FakeGateway) SetFailCalls(n int) { g.mu.Lock(); g.FailCalls = n; g.mu.Unlock() }

func (g *FakeGateway) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Calls, g.FailCalls = 0, 0
}

// stateMu guards the scripted-failure counter and the recorded receipt:
// the reactor goroutine writes them while the test polls — the race
// detector rightly objected to the bare package vars.
var stateMu sync.Mutex
var failReactAfterCapture = 0
var lastReceipt string

// SetFailReactAfterCapture scripts the reaction to fail N times after
// the capture call — retries must replay the journaled receipt.
func SetFailReactAfterCapture(n int) { stateMu.Lock(); failReactAfterCapture = n; stateMu.Unlock() }

// SetLastReceipt / LastReceipt record what the reaction saw, for assertions.
func SetLastReceipt(r string) { stateMu.Lock(); lastReceipt = r; stateMu.Unlock() }
func LastReceipt() string     { stateMu.Lock(); defer stateMu.Unlock(); return lastReceipt }

// CaptureOnPaid implements loomgen.CaptureOnPaidReactions. Yours to edit.
type CaptureOnPaid struct{}

func (h *CaptureOnPaid) OnInvoicePaid(ctx context.Context, evt *loom.Event, data *loomgen.InvoicePaid) ([]loom.Command, error) {
	receipt, err := loom.Once(ctx, "gateway_capture", func(ctx context.Context) (string, error) {
		return Gateway.Capture(evt.AggregateID.String(), data.AmountCents)
	})
	if err != nil {
		return nil, err
	}
	stateMu.Lock()
	if failReactAfterCapture > 0 {
		failReactAfterCapture--
		stateMu.Unlock()
		return nil, fmt.Errorf("scripted post-capture failure")
	}
	lastReceipt = receipt
	stateMu.Unlock()
	return nil, nil
}
