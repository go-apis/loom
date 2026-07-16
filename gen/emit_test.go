package gen_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

func ordersSchema(t *testing.T) []byte {
	t.Helper()
	src, err := os.ReadFile("../internal/e2e/orders/schema/orders.loom")
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestOpenAPI(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := gen.OpenAPI(s)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths      map[string]any `json:"paths"`
		Components struct {
			Schemas map[string]any `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/commands/PlaceOrder", "/entities/OrderSummary", "/entities/OrderSummary/{id}", "/aggregates/Order/{id}"} {
		if doc.Paths[path] == nil {
			t.Errorf("missing path %s", path)
		}
	}
	for _, s := range []string{"Order", "OrderSummary", "OrderItem", "PlaceOrderCommand"} {
		if doc.Components.Schemas[s] == nil {
			t.Errorf("missing component schema %s", s)
		}
	}
}

func TestGraphQL(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := gen.GraphQL(s)
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	for _, want := range []string{
		"type Order {",
		"type OrderSummary {",
		"input PlaceOrderInput {",
		"aggregateId: UUID!",
		"placeOrder(input: PlaceOrderInput!): DispatchResult!",
		"orderSummarys(namespace: String!, where: [FilterInput!]",
		"orderSummaryChanged(namespace: String!, id: UUID!): OrderSummary!",
		"items: [OrderItemInput!]!",
		"customerId: UUID!",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestFoldersLayout(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := gen.Generate(s, gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders", Layout: "folders"})
	if err != nil {
		t.Fatal(err)
	}
	// stubs land in per-kind packages, registry at the root
	for _, want := range []string{
		"aggregates/order.go",
		"policies/scheduleautocancel.go",
		"processes/shiponpayment.go",
		"registry.go",
	} {
		found := false
		for _, w := range res.Written {
			if strings.HasSuffix(w, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing %s in %v", want, res.Written)
		}
	}
	stub, err := os.ReadFile(dir + "/aggregates/order.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stub), "package aggregates") {
		t.Fatalf("stub package: %s", stub)
	}
	reg, err := os.ReadFile(dir + "/registry.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"package orders", `"example.com/orders/aggregates"`, "&aggregates.Order{}", "&processes.ShipOnPayment{}", "&policies.ScheduleAutoCancel{}"} {
		if !strings.Contains(string(reg), want) {
			t.Fatalf("registry missing %q:\n%s", want, reg)
		}
	}
}
