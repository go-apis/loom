package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestGatewayAuth proves the gateway's authorization model: an Auth hook
// resolves each request to an Access; namespaces gate reads and writes,
// Mutate/Mutations gate commands, and All (god mode) unlocks
// cross-namespace list queries with no namespace argument. No hook = the
// open pre-auth gateway (covered by the other gateway tests).
func TestGatewayAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cli := liveOrders(t, ctx)

	// data in two namespaces
	acme, globex := uuid.New(), uuid.New()
	dispatchNS := func(ns string, id uuid.UUID) {
		t.Helper()
		if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
			CommandBase: loom.CommandBase{AggregateID: id, Namespace: ns},
			CustomerId:  uuid.New(),
			Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 100}},
			Currency:    "USD",
		}); err != nil {
			t.Fatal(err)
		}
	}
	dispatchNS("acme", acme)
	dispatchNS("globex", globex)
	waitFor(t, ctx, "both summaries projected", func() bool {
		a, _ := cli.Entity(ctx, "OrderSummary", "acme", acme)
		g, _ := cli.Entity(ctx, "OrderSummary", "globex", globex)
		return a != nil && g != nil
	})

	tokens := map[string]loomgql.Access{
		"root":       {All: true, Mutate: true},
		"acme-rw":    {Namespaces: []string{"acme"}, Mutate: true},
		"acme-ro":    {Namespaces: []string{"acme"}},
		"acme-place": {Namespaces: []string{"acme"}, Mutate: true, Mutations: []string{"placeOrder"}},
	}
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli},
		Auth: func(r *http.Request) (loomgql.Access, error) {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			access, ok := tokens[token]
			if !ok {
				return loomgql.Access{}, fmt.Errorf("bad token")
			}
			return access, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gateway)
	t.Cleanup(srv.Close)

	type gqlResult struct {
		Data   map[string]any   `json:"data"`
		Errors []map[string]any `json:"errors"`
	}
	do := func(token, query string, vars map[string]any) (int, gqlResult) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader(string(body)))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out gqlResult
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	firstError := func(res gqlResult) string {
		if len(res.Errors) == 0 {
			return ""
		}
		return fmt.Sprint(res.Errors[0]["message"])
	}

	listQ := `query($ns: Namespace!) { orderSummarys(namespace: $ns) { id namespace } }`
	placeQ := `mutation($ns: String!, $id: UUID!) { placeOrder(input: {namespace: $ns, aggregateId: $id,
		customerId: "` + uuid.NewString() + `", currency: "USD",
		items: [{sku: "x", quantity: 1, priceCents: 5}]}) { status } }`
	cancelQ := `mutation($ns: String!, $id: UUID!) { cancelOrder(input: {namespace: $ns, aggregateId: $id}) { status } }`

	// no/bad token: rejected before execution
	if code, _ := do("", listQ, map[string]any{"ns": "acme"}); code != http.StatusUnauthorized {
		t.Fatalf("anonymous got %d, want 401", code)
	}

	// namespace scoping on reads
	if _, res := do("acme-ro", listQ, map[string]any{"ns": "acme"}); firstError(res) != "" {
		t.Fatalf("acme-ro read own ns: %s", firstError(res))
	}
	if _, res := do("acme-ro", listQ, map[string]any{"ns": "globex"}); !strings.Contains(firstError(res), "namespace") {
		t.Fatalf("acme-ro read globex should fail, got %q", firstError(res))
	}

	// read-only cannot mutate; scoped rw cannot cross namespaces
	if _, res := do("acme-ro", placeQ, map[string]any{"ns": "acme", "id": uuid.NewString()}); !strings.Contains(firstError(res), "read-only") {
		t.Fatalf("acme-ro mutation should fail read-only, got %q", firstError(res))
	}
	if _, res := do("acme-rw", placeQ, map[string]any{"ns": "globex", "id": uuid.NewString()}); !strings.Contains(firstError(res), "namespace") {
		t.Fatalf("acme-rw into globex should fail, got %q", firstError(res))
	}
	if _, res := do("acme-rw", placeQ, map[string]any{"ns": "acme", "id": uuid.NewString()}); firstError(res) != "" {
		t.Fatalf("acme-rw place in acme: %s", firstError(res))
	}

	// mutation allowlist: placeOrder yes, cancelOrder no
	allowedID := uuid.NewString()
	if _, res := do("acme-place", placeQ, map[string]any{"ns": "acme", "id": allowedID}); firstError(res) != "" {
		t.Fatalf("acme-place placeOrder: %s", firstError(res))
	}
	if _, res := do("acme-place", cancelQ, map[string]any{"ns": "acme", "id": allowedID}); !strings.Contains(firstError(res), "mutation") {
		t.Fatalf("acme-place cancelOrder should fail allowlist, got %q", firstError(res))
	}

	// scoped callers cannot use namespace "*"; god can
	if _, res := do("acme-rw", listQ, map[string]any{"ns": "*"}); !strings.Contains(firstError(res), `"*"`) {
		t.Fatalf("acme-rw star list should fail, got %q", firstError(res))
	}
	_, res := do("root", listQ, map[string]any{"ns": "*"})
	if firstError(res) != "" {
		t.Fatalf("root cross-namespace list: %s", firstError(res))
	}
	rows, _ := res.Data["orderSummarys"].([]any)
	seen := map[string]bool{}
	for _, r := range rows {
		row, _ := r.(map[string]any)
		seen[fmt.Sprint(row["namespace"])] = true
	}
	if !seen["acme"] || !seen["globex"] {
		t.Fatalf("god list missed namespaces: %v", seen)
	}
}
