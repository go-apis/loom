package loomtest_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/loomtest"
)

func TestAggregateScenarios(t *testing.T) {
	reg := billing.NewRegistry()
	customer := uuid.New()
	base := loom.CommandBase{AggregateID: uuid.New(), Namespace: "t"}

	// happy path: fresh invoice raises
	loomtest.Aggregate(t, reg, "Invoice").
		When(&billinggen.RaiseInvoice{CommandBase: base, CustomerId: customer, AmountCents: 4000, Currency: "USD"}).
		Then(&billinggen.InvoiceRaised{Status: "raised", CustomerId: customer, AmountCents: 4000, Currency: "USD"})

	// convergence: raising an existing invoice is a clean no-op
	loomtest.Aggregate(t, reg, "Invoice").
		Given(&billinggen.InvoiceRaised{Status: "raised", CustomerId: customer, AmountCents: 4000, Currency: "USD"}).
		When(&billinggen.RaiseInvoice{CommandBase: base, CustomerId: customer, AmountCents: 4000, Currency: "USD"}).
		ThenNothing()

	// guard: paying an unraised invoice rejects
	loomtest.Aggregate(t, reg, "Invoice").
		When(&billinggen.MarkInvoicePaid{CommandBase: base}).
		ThenError("cannot pay invoice")
}

func TestReactionScenario(t *testing.T) {
	reg := billing.NewRegistry()
	orderID, customer := uuid.New(), uuid.New()

	// billing raises an invoice for every placed order, reusing its id
	loomtest.Reaction(t, reg, "raiseOnOrder").
		When("t", orderID, &billinggen.OrderPlaced{CustomerId: customer, TotalCents: 4000, Currency: "USD"}).
		Then(&billinggen.RaiseInvoice{
			CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "t"},
			CustomerId:  customer, AmountCents: 4000, Currency: "USD",
		})
}
