package e2e_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// Each aggregate and record registers itself from its own generated file;
// NewRegistry adds them in name order, aggregates before records, and an
// owner whose handlers were never bound fails the boot naming it (ADR 0008).
func TestRegistryOwnersInNameOrder(t *testing.T) {
	reg := billing.NewRegistry()
	var aggs, recs, events []string
	for _, a := range reg.Aggregates {
		aggs = append(aggs, a.Name)
	}
	for _, r := range reg.Records {
		recs = append(recs, r.Name)
	}
	for _, e := range reg.Events {
		events = append(events, e.Name)
	}
	if got := strings.Join(aggs, " "); got != "Invoice Payee" {
		t.Fatalf("aggregates %s", got)
	}
	if got := strings.Join(recs, " "); got != "LedgerEntry" {
		t.Fatalf("records %s", got)
	}
	if got := strings.Join(events, " "); got != "InvoicePaid InvoiceRaised LedgerEntryPosted OrderPlaced PayeeRegistered" {
		t.Fatalf("events %s", got)
	}
	if len(reg.Tables) != 1 || reg.Tables[0].Entity != "Payee" {
		t.Fatalf("tables %+v", reg.Tables)
	}

	defer func() {
		msg := fmt.Sprint(recover())
		if !strings.Contains(msg, "aggregate Payee has no handlers") || !strings.Contains(msg, "impl.Payee(") {
			t.Fatalf("want a panic naming Payee, got %q", msg)
		}
	}()
	impl := billinggen.Impl{
		RaiseOnOrder:  &billing.RaiseOnOrder{},
		CaptureOnPaid: &billing.CaptureOnPaid{},
		PostLedger:    &billing.PostLedger{},
	}
	impl.Invoice(&billing.Invoice{})
	impl.LedgerEntry(&billing.LedgerEntry{})
	billinggen.NewRegistry(impl)
}
