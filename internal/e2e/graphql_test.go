package e2e_test

import (
	"bytes"
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
	"github.com/go-apis/loom/internal/e2e/orders"
)

// TestGraphQLGateway drives both services through one composed graph:
// mutations dispatch commands, list queries filter read models, and a
// hand-written join field crosses the service boundary.
func TestGraphQLGateway(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	bus := loom.NewMemoryBus()
	ordersCli, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: orders.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	billingCli, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ordersCli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ordersCli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := billingCli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{ordersCli, billingCli},
		Joins: []loomgql.Join{{
			OnType: "OrderSummary", Field: "invoice", Returns: "Invoice",
			Resolve: func(ctx context.Context, src map[string]any) (any, error) {
				// the invoice reuses the order's aggregate id
				id, err := uuid.Parse(src["id"].(string))
				if err != nil {
					return nil, err
				}
				state, version, err := billingCli.Load(ctx, "Invoice", src["namespace"].(string), id)
				if err != nil || version == 0 {
					return nil, err
				}
				raw, _ := json.Marshal(state)
				doc := map[string]any{}
				_ = json.Unmarshal(raw, &doc)
				return doc, nil
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gateway)
	defer srv.Close()

	gql := func(query string, vars map[string]any) map[string]any {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
		resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Data   map[string]any `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		if len(out.Errors) > 0 {
			t.Fatalf("graphql errors: %+v", out.Errors)
		}
		return out.Data
	}

	// mutation → command dispatch, nested input list included
	orderID := uuid.New()
	data := gql(`mutation($in: PlaceOrderInput!) { placeOrder(input: $in) { status } }`, map[string]any{
		"in": map[string]any{
			"aggregateId": orderID.String(), "namespace": "default",
			"customerId": uuid.NewString(), "currency": "USD",
			"items": []any{map[string]any{"sku": "widget", "quantity": 2, "priceCents": 750}},
		},
	})
	if data["placeOrder"].(map[string]any)["status"] != "ok" {
		t.Fatalf("placeOrder: %+v", data)
	}

	// billing raised the invoice off the bus; pay it through the graph too
	waitFor(t, ctx, "invoice raised", func() bool {
		_, version, err := billingCli.Load(ctx, "Invoice", "default", orderID)
		return err == nil && version > 0
	})
	gql(`mutation($in: MarkInvoicePaidInput!) { markInvoicePaid(input: $in) { status } }`, map[string]any{
		"in": map[string]any{"aggregateId": orderID.String(), "namespace": "default"},
	})
	waitFor(t, ctx, "order shipped", func() bool {
		state, _, err := ordersCli.Load(ctx, "Order", "default", orderID)
		return err == nil && strings.Contains(toJSON(state), "shipped")
	})

	// filtered list query + camelCase fields + the cross-service join
	waitFor(t, ctx, "summary queryable through the graph", func() bool {
		data := gql(`query($ns: String!) {
			orderSummarys(namespace: $ns, where: [{field: "status", op: EQ, value: "shipped"}]) {
				id totalCents status invoice { status amountCents }
			}
		}`, map[string]any{"ns": "default"})
		items, _ := data["orderSummarys"].([]any)
		if len(items) != 1 {
			return false
		}
		row := items[0].(map[string]any)
		inv, _ := row["invoice"].(map[string]any)
		return row["totalCents"] == float64(1500) && inv != nil &&
			inv["status"] == "paid" && inv["amountCents"] == float64(1500)
	})

	// aggregate get through the graph
	data = gql(`query($ns: String!, $id: UUID!) { order(namespace: $ns, id: $id) { status currency } }`,
		map[string]any{"ns": "default", "id": orderID.String()})
	ord, _ := data["order"].(map[string]any)
	if ord == nil || ord["status"] != "shipped" || ord["currency"] != "USD" {
		t.Fatalf("order query: %+v", data)
	}

	// introspection works (clients and IDEs depend on it)
	data = gql(`{ __schema { queryType { name } } }`, nil)
	if data["__schema"].(map[string]any)["queryType"].(map[string]any)["name"] != "Query" {
		t.Fatalf("introspection: %+v", data)
	}
}

func toJSON(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
