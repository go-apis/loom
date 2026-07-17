package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/billing"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
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
	ordersCli, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
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

	// upload mutation: session brokered through the graph, FileRef and the
	// Long-typed size come back
	data = gql(`mutation($in: CreateContractUploadInput!) {
		createContractUpload(input: $in) {
			url
			protocol
			file { id key name contentType size downloadUrl }
		}
	}`, map[string]any{
		"in": map[string]any{
			"namespace": "default", "id": orderID.String(),
			"name": "contract.pdf", "contentType": "application/pdf", "size": 4096,
		},
	})
	sess, _ := data["createContractUpload"].(map[string]any)
	file, _ := sess["file"].(map[string]any)
	if sess["url"] == "" || sess["protocol"] != loom.ProtocolGCSResumable ||
		file == nil || file["name"] != "contract.pdf" || file["size"] != float64(4096) ||
		!strings.Contains(file["key"].(string), "orders/default/"+orderID.String()+"/Contract/") ||
		!strings.HasPrefix(file["downloadUrl"].(string), "/files?key=orders%2Fdefault%2F") {
		t.Fatalf("createContractUpload: %+v", data)
	}
	// `on started` reached the domain as an event
	waitFor(t, ctx, "contract requested", func() bool {
		_, version, err := ordersCli.Load(ctx, "Order", "default", orderID)
		return err == nil && version >= 3
	})

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

// TestGraphQLPlayground: a browser GET serves the embedded IDE; curl GET
// with ?query= still executes.
func TestGraphQLPlayground(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gateway)
	defer srv.Close()

	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL, nil)
	req.Header.Set("Accept", "text/html")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(page), "loom graphql") || !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("playground: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	resp, err = http.Get(srv.URL + "?query=" + url.QueryEscape(`{ __schema { queryType { name } } }`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `"Query"`) {
		t.Fatalf("GET query: %s", body)
	}
}

// readSSE reads one non-heartbeat SSE event (event name + data line).
func readSSE(t *testing.T, scanner *bufio.Scanner) (event, data string) {
	t.Helper()
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		case line == "" && event != "":
			return event, data
		}
	}
	t.Fatalf("stream ended early: %v", scanner.Err())
	return "", ""
}

// TestGraphQLSubscriptions: {x}Changed rides SSE on /graphql — the
// current doc immediately, then a fresh execution per change, ending
// when the client disconnects. The gateway is the only public surface.
func TestGraphQLSubscriptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gateway)
	defer srv.Close()

	orderID := uuid.New()
	place := &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
		CustomerId:  uuid.New(),
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 2, PriceCents: 750}},
		Currency:    "USD",
	}
	if err := cli.Dispatch(ctx, place); err != nil {
		t.Fatal(err)
	}

	vars, _ := json.Marshal(map[string]any{"ns": "default", "id": orderID.String()})
	q := `subscription($ns: String!, $id: UUID!) { orderChanged(namespace: $ns, id: $id) { status totalCents } }`
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
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type: %q", ct)
	}
	scanner := bufio.NewScanner(resp.Body)

	next := func() map[string]any {
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
		doc, _ := res.Data["orderChanged"].(map[string]any)
		if doc == nil {
			t.Fatalf("no orderChanged in %s", data)
		}
		return doc
	}

	// the current state arrives without waiting for a change
	if doc := next(); doc["status"] != "placed" || doc["totalCents"] != float64(1500) {
		t.Fatalf("initial: %+v", doc)
	}
	// a dispatch wakes the watch and re-executes the selection
	if err := cli.Dispatch(ctx, &ordersgen.ShipOrder{CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	if doc := next(); doc["status"] != "shipped" {
		t.Fatalf("after ship: %+v", doc)
	}
}

// TestGatewayStreams: the raw SSE watch passthrough serves entity and
// aggregate streams by service; everything else on the service surface
// stays unreachable through it.
func TestGatewayStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(loomgql.Streams(cli))
	defer srv.Close()

	orderID := uuid.New()
	if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "default"},
		CustomerId:  uuid.New(),
		Items:       []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 100}},
		Currency:    "USD",
	}); err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		srv.URL+"/orders/aggregates/Order/"+orderID.String()+"/stream?namespace=default", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	event, data := readSSE(t, bufio.NewScanner(resp.Body))
	if event != "change" || !strings.Contains(data, `"placed"`) {
		t.Fatalf("watch stream: %s %s", event, data)
	}

	// ops endpoints don't pass through
	for _, path := range []string{"/orders/events", "/orders/stats", "/orders/console", "/nope/entities/X/1/stream"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: status %d, want 404", path, resp.StatusCode)
		}
	}
}
