package e2e_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-apis/loom"
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
	if got := strings.Join(aggs, " "); got != "Contact Invoice Payee" {
		t.Fatalf("aggregates %s", got)
	}
	if got := strings.Join(recs, " "); got != "LedgerEntry" {
		t.Fatalf("records %s", got)
	}
	if got := strings.Join(events, " "); got != "ContactRecorded InvoicePaid InvoiceRaised LedgerEntryPosted OrderPlaced PayeeRegistered" {
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
	impl.Contact(&billing.Contact{})
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

// NewPartialRegistry is the whole registry from an Impl that binds part
// of it, where NewRegistry panics naming the first it misses: every
// declaration is there with its definition, one that is bound reacts,
// and one that is not panics only when it is called.
func TestPartialRegistryHoldsEveryDeclaration(t *testing.T) {
	impl := *new(billinggen.Impl).Invoice(&billing.Invoice{}).RaiseOnOrder(&billing.RaiseOnOrder{})
	reg, whole := billinggen.NewPartialRegistry(impl), billing.NewRegistry()
	if len(reg.Aggregates) != len(whole.Aggregates) || len(reg.Records) != len(whole.Records) || len(reg.Policies) != len(whole.Policies) ||
		len(reg.Processes) != len(whole.Processes) || len(reg.Events) != len(whole.Events) || len(reg.Tables) != len(whole.Tables) {
		t.Fatalf("the partial registry is not the whole one: %d aggregates, %d records, %d policies, %d processes, %d events, %d tables",
			len(reg.Aggregates), len(reg.Records), len(reg.Policies), len(reg.Processes), len(reg.Events), len(reg.Tables))
	}
	var unbound *loom.ReactorDef
	for _, p := range reg.Policies {
		if p.Name == "postLedger" {
			unbound = p
		}
	}
	if unbound == nil || len(unbound.Events) == 0 || unbound.Events[0] != "InvoicePaid" {
		t.Fatalf("the policy nothing is bound to keeps its definition: %+v", unbound)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("a policy nothing is bound to reacted")
			}
		}()
		_, _ = unbound.React(context.Background(), &loom.Event{Type: "InvoicePaid", Data: &billinggen.InvoicePaid{}})
	}()
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "has no implementation") {
			t.Fatalf("NewRegistry on the same Impl: %v", r)
		}
	}()
	billinggen.NewRegistry(impl)
}
