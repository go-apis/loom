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
		if !strings.Contains(msg, "aggregate Payee has no implementation") || !strings.Contains(msg, "impl.Payee(") {
			t.Fatalf("want a panic naming Payee, got %q", msg)
		}
	}()
	impl := billinggen.Impl{}
	impl.PostLedger(&billing.PostLedger{})
	impl.RaiseOnOrder(&billing.RaiseOnOrder{})
	impl.CaptureOnPaid(&billing.CaptureOnPaid{})
	impl.Invoice(&billing.Invoice{})
	impl.LedgerEntry(&billing.LedgerEntry{})
	billinggen.NewRegistry(impl)
}

// A binding method answers its Impl, so a whole Impl is one expression;
// BoundBy reads what is bound by the method's own name, nil when nothing
// is, so a caller can wrap a handler before the Impl becomes a registry.
func TestBindingsChainAndAreReadBack(t *testing.T) {
	invoice, payee := &billing.Invoice{}, &billing.Payee{}
	impl := *new(billinggen.Impl).Invoice(invoice).Payee(payee)
	bound := billinggen.BoundBy(&impl)
	if bound.Invoice() != billinggen.InvoiceHandlers(invoice) || bound.Payee() != billinggen.PayeeHandlers(payee) {
		t.Fatalf("what was bound is not what is read back: %v %v", bound.Invoice(), bound.Payee())
	}
	if bound.LedgerEntry() != nil || bound.PostLedger() != nil || bound.RaiseOnOrder() != nil {
		t.Fatal("a declaration nothing bound reads back something")
	}

	// wrap one and bind the wrapper: the registry is built from it
	wrapped := &countingInvoice{InvoiceHandlers: bound.Invoice()}
	impl.Invoice(wrapped)
	if billinggen.BoundBy(&impl).Invoice() != billinggen.InvoiceHandlers(wrapped) {
		t.Fatal("the wrapper is not what is bound now")
	}
}

type countingInvoice struct {
	billinggen.InvoiceHandlers
}
