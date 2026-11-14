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
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
	loommcp "github.com/go-apis/loom/mcp"
)

// bearer sends every request of an MCP client under one token.
type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

// TestMCPReachesTheGatewayUnderItsRules: the five tools reach the
// composed schema — an agent finds a command, reads what it takes, runs
// it and reads the result back — and every call answers to the
// gateway's own Auth and @role gates: a caller the API would refuse is
// refused here in the same words, and one with no token never reaches a
// tool.
func TestMCPReachesTheGatewayUnderItsRules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	depot := uuid.NewString()
	tokens := map[string]loomgql.Access{
		"owner":  {Namespaces: []string{"acme"}, Mutate: true, Roles: map[string]string{"acme": "owner"}},
		"member": {Namespaces: []string{"acme"}, Mutate: true, Roles: map[string]string{"acme": "member"}},
		"other":  {Namespaces: []string{"globex"}, Mutate: true, Roles: map[string]string{"globex": "owner"}},
		"reader": {Namespaces: []string{"acme"}, Roles: map[string]string{"acme": "owner"}},
		"courier": {Namespaces: []string{"acme"}, Mutate: true, Roles: map[string]string{"acme": "member"},
			Attrs: map[string]string{"kind": "courier", "depot": depot}},
	}
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli},
		Auth: func(r *http.Request) (loomgql.Access, error) {
			a, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
			if !ok {
				return loomgql.Access{}, fmt.Errorf("bad token")
			}
			return a, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agents, err := loommcp.New(loommcp.Config{Gateway: gateway, Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(agents)
	defer srv.Close()

	// no token: refused before any tool, with a challenge
	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("no token: status %d, challenge %q; want 401 and a challenge", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}

	session := func(token string) *sdk.ClientSession {
		t.Helper()
		c := sdk.NewClient(&sdk.Implementation{Name: "test"}, nil)
		cs, err := c.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL, HTTPClient: &http.Client{Transport: bearer{token}}, DisableStandaloneSSE: true}, nil)
		if err != nil {
			t.Fatalf("connect as %s: %v", token, err)
		}
		t.Cleanup(func() { cs.Close() })
		return cs
	}
	// call answers a tool's text and whether the tool refused
	call := func(cs *sdk.ClientSession, tool string, args map[string]any) (string, bool) {
		t.Helper()
		res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatalf("%s: %v", tool, err)
		}
		// the answer is the first part; a note of what was left out
		// because it is hidden from the caller may follow it
		if len(res.Content) == 0 {
			t.Fatalf("%s: no content", tool)
		}
		return res.Content[0].(*sdk.TextContent).Text, res.IsError
	}
	decode := func(text string) map[string]any {
		t.Helper()
		out := map[string]any{}
		if err := json.Unmarshal([]byte(text), &out); err != nil {
			t.Fatalf("not an object: %s", text)
		}
		return out
	}

	owner := session("owner")

	// the tool list is the five, whatever the schema holds
	tools, err := owner.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}
	if got := strings.Join(names, " "); got != "describe dispatch get list search" {
		t.Fatalf("tools = %q", got)
	}

	// find the command, and learn which tool calls it
	text, refused := call(owner, "search", map[string]any{"query": "place order"})
	if refused || !strings.Contains(text, `"name":"placeOrder"`) || !strings.Contains(text, `"tool":"dispatch"`) {
		t.Fatalf("search = %s", text)
	}
	// what it takes: the input object expanded, nested items included
	text, refused = call(owner, "describe", map[string]any{"name": "placeOrder"})
	if refused {
		t.Fatalf("describe refused: %s", text)
	}
	for _, want := range []string{`"type":"PlaceOrderInput!"`, `"name":"customerId"`, `"name":"sku"`} {
		if !strings.Contains(text, want) {
			t.Errorf("describe placeOrder lacks %s: %s", want, text)
		}
	}

	// what the schema's doc comments say reaches the agent: search
	// matches on a word only the doc holds and shows its first line, and
	// describe carries the docs of the command, its fields, a nested
	// type's fields, an enum, and the fields of what a read returns. A
	// plain `//` note in the schema (ShipOrder's) is never shown.
	text, refused = call(owner, "search", map[string]any{"query": "nobody pays"})
	if refused || !strings.Contains(text, `"name":"placeOrder"`) || !strings.Contains(text, `"summary":"Place a new order for a customer. The total is worked out from the items."`) {
		t.Errorf("search by a doc's words = %s", text)
	}
	text, _ = call(owner, "describe", map[string]any{"name": "placeOrder"})
	for _, want := range []string{
		`"description":"Place a new order for a customer. The total is worked out from the items.\n\nAn order nobody pays for is cancelled on its own after a while."`,
		`"description":"ISO 4217 code the prices are in, for example USD."`,
		`"description":"The stock-keeping unit being bought."`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("describe placeOrder lacks %s: %s", want, text)
		}
	}
	text, _ = call(owner, "describe", map[string]any{"name": "order"})
	for _, want := range []string{`One Order by id. A customer's order, from placed to shipped or cancelled.`, `"description":"The sum of every item's price times its quantity."`} {
		if !strings.Contains(text, want) {
			t.Errorf("describe order lacks %s: %s", want, text)
		}
	}
	if text, _ = call(owner, "describe", map[string]any{"name": "Priority"}); !strings.Contains(text, `"description":"How soon an order ships."`) {
		t.Errorf("describe Priority = %s", text)
	}
	if text, _ = call(owner, "describe", map[string]any{"name": "shipOrder"}); strings.Contains(text, "description") {
		t.Errorf("a plain comment reached a caller: %s", text)
	}

	// each caller is shown what it could run, by the gateway's own
	// rules: the role a command asks for, the attributes an @allow rule
	// asks for, and whether the caller may change anything at all
	found := func(token, query string) string {
		t.Helper()
		text, refused := call(session(token), "search", map[string]any{"query": query, "limit": 100})
		if refused {
			t.Fatalf("search as %s: %s", token, text)
		}
		var names []string
		for _, op := range decode(text)["operations"].([]any) {
			names = append(names, op.(map[string]any)["name"].(string))
		}
		return " " + strings.Join(names, " ") + " "
	}
	for _, c := range []struct {
		token, query, op string
		shown            bool
	}{
		{"owner", "ship", "shipOrder", true},
		{"member", "ship", "shipOrder", false}, // @role(owner, shipper)
		{"member", "place", "placeOrder", true},
		{"reader", "place", "placeOrder", false}, // may change nothing
		{"reader", "order", "orderSummarys", true},
		{"member", "customer", "customerSpend", false}, // entity @role(owner)
		{"owner", "customer", "customerSpend", true},
		{"owner", "parcel", "registerParcel", false}, // @allow, and the owner has no attributes
		{"owner", "parcel", "parcels", false},
		{"courier", "parcel", "registerParcel", true},
		{"courier", "parcel", "parcels", true},
		{"courier", "ship", "shipOrder", false},
	} {
		if got := strings.Contains(found(c.token, c.query), " "+c.op+" "); got != c.shown {
			t.Errorf("%s searching %q: %s shown = %v, want %v", c.token, c.query, c.op, got, c.shown)
		}
	}
	// and what it is not shown, it cannot describe
	if text, refused = call(session("member"), "describe", map[string]any{"name": "shipOrder"}); !refused || !strings.Contains(text, "no operation") {
		t.Errorf("a member describing shipOrder = %s", text)
	}

	// run it
	orderID := uuid.New()
	place := map[string]any{"name": "placeOrder", "args": map[string]any{"input": map[string]any{
		"aggregateId": orderID.String(), "namespace": "acme",
		"customerId": uuid.NewString(), "currency": "USD",
		"items": []any{map[string]any{"sku": "widget", "quantity": 2, "priceCents": 750}},
	}}}
	if text, refused = call(owner, "dispatch", place); refused || decode(text)["status"] != "ok" {
		t.Fatalf("dispatch placeOrder = %s", text)
	}

	// read it back: one record with its plain fields, then a chosen few
	one := map[string]any{"name": "order", "args": map[string]any{"namespace": "acme", "id": orderID.String()}}
	if text, refused = call(owner, "get", one); refused || decode(text)["status"] != "placed" {
		t.Fatalf("get order = %s", text)
	}
	one["fields"] = []any{"status", "items.sku"}
	text, refused = call(owner, "get", one)
	if refused || text != `{"items":[{"sku":"widget"}],"status":"placed"}` {
		t.Fatalf("get order, chosen fields = %s", text)
	}
	waitFor(t, ctx, "the summary projected", func() bool {
		text, refused := call(owner, "list", map[string]any{"name": "orderSummarys", "args": map[string]any{"namespace": "acme"}})
		return !refused && strings.Contains(text, orderID.String())
	})

	// the wrong tool for the operation names the right one
	if text, refused = call(owner, "get", place); !refused || !strings.Contains(text, "dispatch tool") {
		t.Errorf("a command through get = %s", text)
	}
	// nothing a caller writes reaches the document unchecked
	one["fields"] = []any{"status } __schema { types { name"}
	if text, refused = call(owner, "get", one); !refused || !strings.Contains(text, "no field") {
		t.Errorf("a made-up field = %s", text)
	}
	if text, refused = call(owner, "dispatch", map[string]any{"name": "placeOrder", "args": map[string]any{"input) { status } } #": 1}}); !refused || !strings.Contains(text, "takes no argument") {
		t.Errorf("a made-up argument = %s", text)
	}

	// the gateway's rules, not this package's: another tenant sees
	// nothing and changes nothing, and a member without the role the
	// command asks for is refused — each in the API's own words
	ship := map[string]any{"name": "shipOrder", "args": map[string]any{"input": map[string]any{"aggregateId": orderID.String(), "namespace": "acme"}}}
	direct := func(token, doc string, vars map[string]any) string {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		ex, _ := loomgql.AsExecutor(gateway)
		r, err := ex.Authenticate(r)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(ex.Exec(r.Context(), doc, vars).Errors, "\n")
	}
	shipDoc := `mutation($input: ShipOrderInput!) { shipOrder(input: $input) { status } }`
	for _, token := range []string{"other", "member"} {
		want := direct(token, shipDoc, ship["args"].(map[string]any))
		if want == "" {
			t.Fatalf("the API let %s ship the order", token)
		}
		if text, refused = call(session(token), "dispatch", ship); !refused || text != want {
			t.Errorf("%s shipping through MCP = %q (refused %v); the API says %q", token, text, refused, want)
		}
	}
	if text, refused = call(session("other"), "get", map[string]any{"name": "order", "args": map[string]any{"namespace": "acme", "id": orderID.String()}}); !refused {
		t.Errorf("another tenant read the order: %s", text)
	}
	if text, refused = call(owner, "dispatch", ship); refused || decode(text)["status"] != "ok" {
		t.Errorf("the owner shipping = %s", text)
	}

	// a @plan command through dispatch answers the whole plan — its
	// events included, with nothing asked for — and runs nothing; the
	// plan's id through runPlan runs it
	courier := session("courier")
	parcel := uuid.NewString()
	if text, refused = call(courier, "dispatch", map[string]any{"name": "registerParcel", "args": map[string]any{"input": map[string]any{
		"aggregateId": parcel, "namespace": "acme", "depotId": depot, "label": "box"}}}); refused {
		t.Fatalf("registerParcel = %s", text)
	}
	if text, _ = call(courier, "describe", map[string]any{"name": "discardParcel"}); !strings.Contains(text, "This does not run") || !strings.Contains(text, "runPlan") {
		t.Errorf("describe discardParcel does not say it plans: %s", text)
	}
	text, refused = call(courier, "dispatch", map[string]any{"name": "discardParcel", "args": map[string]any{"input": map[string]any{"aggregateId": parcel, "namespace": "acme"}}})
	if refused || !strings.Contains(text, `"events":[{"data":{"discarded":true},"published":false,"reactions":[],"type":"ParcelDiscarded"}]`) {
		t.Fatalf("dispatch discardParcel = %s", text)
	}
	one = map[string]any{"name": "parcel", "args": map[string]any{"namespace": "acme", "id": parcel}}
	if text, _ := call(courier, "get", one); decode(text)["discarded"] == true {
		t.Fatalf("planning discarded the parcel: %s", text)
	}
	planID := decode(text)["id"]
	if text, refused = call(courier, "dispatch", map[string]any{"name": "runPlan", "args": map[string]any{"namespace": "acme", "id": planID}}); refused || decode(text)["status"] != "ok" {
		t.Fatalf("dispatch runPlan = %s", text)
	}
	if text, _ := call(courier, "get", one); decode(text)["discarded"] != true {
		t.Errorf("the plan ran and the parcel is not discarded: %s", text)
	}
}
