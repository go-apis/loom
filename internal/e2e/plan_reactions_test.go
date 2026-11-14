package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestAPlanNamesWhatReacts: a plan lists, for each event the command
// would emit, what the schema says reacts to it — policies with the
// commands they may dispatch, processes with the outside calls they
// declare, projections with the read model they update — and whether
// the event goes to the bus. Another service's process that consumes
// the event is named by the service that has it. Nothing is run.
func TestAPlanNamesWhatReacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	bus := loom.NewMemoryBus()
	ord, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	bill, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ord.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	said := func(reactions []loom.PlannedReaction) string {
		var out []string
		for _, r := range reactions {
			s := r.Kind + " " + r.Name
			if r.Service != "" {
				s += " in " + r.Service
			}
			if len(r.Dispatches) > 0 {
				s += " -> " + strings.Join(r.Dispatches, ",")
			}
			if len(r.Effects) > 0 {
				s += " calls " + strings.Join(r.Effects, ",")
			}
			if r.Entity != "" {
				s += " updates " + r.Entity
			}
			out = append(out, s)
		}
		return strings.Join(out, "; ")
	}

	// placing an order: a policy that may cancel it later, two read
	// models, and the bus
	place := &ordersgen.PlaceOrder{CustomerId: uuid.New(), Currency: "USD", Items: []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 500}}}
	place.Namespace, place.AggregateID = "acme", uuid.New()
	plan, err := ord.Plan(ctx, place, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Events) != 1 || plan.Events[0].Type != "OrderPlaced" || !plan.Events[0].Published {
		t.Fatalf("plan events = %+v", plan.Events)
	}
	if got, want := said(plan.Events[0].Reactions), "policy scheduleAutoCancel -> CancelOrder; projection customerSpend updates CustomerSpend; projection orderSummary updates OrderSummary"; got != want {
		t.Errorf("OrderPlaced's reactions:\n got %s\nwant %s", got, want)
	}
	// and nothing of it happened
	if _, version, err := ord.Load(ctx, "Order", "acme", place.AggregateID); err != nil || version != 0 {
		t.Errorf("planning placed the order: v%d %v", version, err)
	}

	// the service that consumes it says so
	if got, want := said(bill.ConsumedReactions("orders", "OrderPlaced")), "process raiseOnOrder in billing -> RaiseInvoice"; got != want {
		t.Errorf("billing's reactions to orders' OrderPlaced: got %s, want %s", got, want)
	}
	if got := bill.ConsumedReactions("orders", "OrderShipped"); len(got) != 0 {
		t.Errorf("billing reacts to OrderShipped: %+v", got)
	}
	if got := ord.Reactions("InvoicePaid"); len(got) != 0 {
		t.Errorf("orders lists a consumed event's reactions as its own: %+v", got)
	}

	// paying an invoice: a process, and the outside call it declares
	invoice := uuid.New()
	if err := bill.Dispatch(ctx, &billinggen.RaiseInvoice{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "acme"}, CustomerId: uuid.New(), AmountCents: 4200, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	paid, err := bill.Plan(ctx, &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "acme"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := said(paid.Events[0].Reactions); !strings.Contains(got, "process captureOnPaid calls gateway_capture") {
		t.Errorf("InvoicePaid's reactions = %s; want the capture process and its outside call", got)
	}

	// through the gateway: the plan's events carry them
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{ord, bill},
		Auth: func(r *http.Request) (loomgql.Access, error) {
			return loomgql.Access{Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "admin"}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ex, _ := loomgql.AsExecutor(gateway)
	r, err := ex.Authenticate(httptest.NewRequest(http.MethodPost, "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	parcel := uuid.NewString()
	if res := ex.Exec(r.Context(), `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`,
		map[string]any{"in": map[string]any{"aggregateId": parcel, "namespace": "acme", "depotId": uuid.NewString(), "label": "box"}}); len(res.Errors) > 0 {
		t.Fatal(res.Errors)
	}
	res := ex.Exec(r.Context(), `mutation($in: DiscardParcelInput!) { discardParcel(input: $in) {
		events { type published reactions { kind name service dispatches effects entity } } } }`,
		map[string]any{"in": map[string]any{"aggregateId": parcel, "namespace": "acme"}})
	if len(res.Errors) > 0 {
		t.Fatal(res.Errors)
	}
	raw, _ := json.Marshal(res.Data["discardParcel"])
	if got, want := string(raw), `{"events":[{"published":false,"reactions":[],"type":"ParcelDiscarded"}]}`; got != want {
		t.Errorf("discardParcel's plan = %s, want %s", got, want)
	}
}
