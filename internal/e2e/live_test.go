package e2e_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

func liveOrders(t *testing.T, ctx context.Context) *loom.Client {
	t.Helper()
	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return cli
}

func placeOrder(t *testing.T, ctx context.Context, cli *loom.Client, orderID, customerID uuid.UUID, cents int64) {
	t.Helper()
	if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
		CustomerId:  customerID,
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: cents}},
		Currency:    "USD",
	}); err != nil {
		t.Fatal(err)
	}
}

// TestKeyedFoldProjection: key(customer_id) routes many orders' events
// onto one customer-keyed row, and the @fold stub does the counting no
// assignment fold could — the 1-* read-model answer.
func TestKeyedFoldProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli := liveOrders(t, ctx)

	customer := uuid.New()
	placeOrder(t, ctx, cli, uuid.New(), customer, 700)
	placeOrder(t, ctx, cli, uuid.New(), customer, 300)
	placeOrder(t, ctx, cli, uuid.New(), uuid.New(), 999) // someone else

	waitFor(t, ctx, "customer spend folded", func() bool {
		state, err := cli.Entity(ctx, "CustomerSpend", "default", customer)
		if err != nil || state == nil {
			return false
		}
		spend := state.(*ordersgen.CustomerSpend)
		return spend.OrderCount == 2 && spend.SpendCents == 1000
	})
}

// TestGraphQLListSubscription: {x}sChanged is a live filtered query —
// the masspayout-screen shape. The empty list arrives first, then the
// whole fresh list whenever a row enters the filter.
func TestGraphQLListSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli := liveOrders(t, ctx)

	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gateway)
	defer srv.Close()

	customer := uuid.New()
	q := `subscription($ns: Namespace!, $cust: String!) {
		orderSummarysChanged(namespace: $ns, where: [{field: "customer_id", op: EQ, value: $cust}]) {
			id status totalCents
		}
	}`
	vars, _ := json.Marshal(map[string]any{"ns": "default", "cust": customer.String()})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		srv.URL+"?query="+url.QueryEscape(q)+"&variables="+url.QueryEscape(string(vars)), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)

	next := func() []any {
		event, data := readSSE(t, scanner)
		if event != "next" {
			t.Fatalf("want next, got %q: %s", event, data)
		}
		var res struct {
			Data   map[string]any `json:"data"`
			Errors []any          `json:"errors"`
		}
		if err := json.Unmarshal([]byte(data), &res); err != nil || len(res.Errors) > 0 {
			t.Fatalf("bad result %s (err=%v)", data, err)
		}
		list, _ := res.Data["orderSummarysChanged"].([]any)
		return list
	}

	// the filter matches nothing yet: the initial emit is the empty list
	if list := next(); len(list) != 0 {
		t.Fatalf("initial: %+v", list)
	}
	// a row entering the filter re-sends the whole list
	orderID := uuid.New()
	placeOrder(t, ctx, cli, orderID, customer, 1500)
	list := next()
	for len(list) == 0 { // the wake can beat the projection fold
		list = next()
	}
	row, _ := list[0].(map[string]any)
	if len(list) != 1 || row["id"] != orderID.String() || row["status"] != "placed" || row["totalCents"] != float64(1500) {
		t.Fatalf("after place: %+v", list)
	}
	// another customer's order does not disturb this filter (no emit
	// expected — verified indirectly: the next emit is ours going away)
	placeOrder(t, ctx, cli, uuid.New(), uuid.New(), 42)
	if err := cli.Dispatch(ctx, &ordersgen.CancelOrder{CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	for {
		list = next()
		if len(list) == 1 {
			if row, _ := list[0].(map[string]any); row["status"] == "cancelled" {
				break
			}
		}
	}
}
