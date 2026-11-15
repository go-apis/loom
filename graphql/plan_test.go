package graphql

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
)

// TestPlanDocCarriesWhatReactionsDecided: a plan rendered for the
// gateway keeps, for each event, what emits it and what caused that,
// and for each reaction what the plan found when it ran it — the
// commands it decided on, the outside call it stopped at — with another
// service's process, which no plan runs, listed after and without an
// outcome.
func TestPlanDocCarriesWhatReactionsDecided(t *testing.T) {
	id, target := uuid.MustParse("11111111-1111-4111-8111-111111111111"), uuid.MustParse("22222222-2222-4222-8222-222222222222")
	at := time.Date(2026, 11, 15, 3, 0, 0, 0, time.UTC)
	plan := &loom.Plan{ID: id, Command: "MarkInvoicePaid", Namespace: "acme", TargetID: target, Version: 1, ExpiresAt: at, Events: []loom.PlannedEvent{
		{Type: "InvoicePaid", Data: map[string]any{"amount_cents": 4200}, Aggregate: "Invoice", AggregateID: target, Command: "MarkInvoicePaid", Published: true, Reactions: []loom.PlannedReaction{
			{Kind: "policy", Name: "postLedger", Dispatches: []string{"PostLedgerEntry"}, Outcome: loom.OutcomeDecided, Commands: []loom.PlannedDispatch{
				{Command: "PostLedgerEntry", Data: map[string]any{"amount_cents": 4200}, Namespace: "acme", TargetID: target, How: loom.DispatchRun},
				{Command: "RemindCustomer", Data: map[string]any{}, Namespace: "acme", TargetID: target, How: loom.DispatchTimer, At: &at},
			}},
			{Kind: "process", Name: "captureOnPaid", Effects: []string{"gateway_capture"}, Outcome: loom.OutcomeCalls, Calls: []string{"gateway_capture"}, Detail: "depends on the call"},
		}},
		{Type: "LedgerEntryPosted", Data: map[string]any{}, Aggregate: "LedgerEntry", AggregateID: target, Command: "PostLedgerEntry", Cause: "policy postLedger"},
	}}
	doc, err := planDoc(plan, func(eventType string) []loom.PlannedReaction {
		if eventType != "InvoicePaid" {
			return nil
		}
		return []loom.PlannedReaction{{Kind: "process", Name: "shipOnPayment", Service: "orders", Dispatches: []string{"ShipOrder"}}}
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(doc["events"])
	want := `[{"aggregate":"Invoice","aggregateId":"22222222-2222-4222-8222-222222222222","command":"MarkInvoicePaid","data":{"amount_cents":4200},"published":true,"reactions":[` +
		`{"calls":[],"commands":[{"command":"PostLedgerEntry","data":{"amount_cents":4200},"how":"run","namespace":"acme","targetId":"22222222-2222-4222-8222-222222222222"},` +
		`{"at":"2026-11-15T03:00:00Z","command":"RemindCustomer","data":{},"how":"timer","namespace":"acme","targetId":"22222222-2222-4222-8222-222222222222"}],"dispatches":["PostLedgerEntry"],"effects":[],"kind":"policy","name":"postLedger","outcome":"decided"},` +
		`{"calls":["gateway_capture"],"commands":[],"detail":"depends on the call","dispatches":[],"effects":["gateway_capture"],"kind":"process","name":"captureOnPaid","outcome":"calls"},` +
		`{"calls":[],"commands":[],"dispatches":["ShipOrder"],"effects":[],"kind":"process","name":"shipOnPayment","service":"orders"}],"type":"InvoicePaid"},` +
		`{"aggregate":"LedgerEntry","aggregateId":"22222222-2222-4222-8222-222222222222","cause":"policy postLedger","command":"PostLedgerEntry","data":{},"published":false,"reactions":[],"type":"LedgerEntryPosted"}]`
	if got := string(raw); got != want {
		t.Errorf("the plan's events:\n got %s\nwant %s", got, want)
	}
}
