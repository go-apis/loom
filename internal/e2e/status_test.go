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
	"github.com/go-apis/loom/internal/e2e/billing"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
	loommcp "github.com/go-apis/loom/mcp"
)

// TestServiceStatus: Status says what a service's background work is
// doing. Before any instance runs, nobody holds the lease and every
// runner is behind an event, since when; once one runs it leads, the
// runners catch up, and the timer the order scheduled is pending. The
// gateway serves it to All access only, and the MCP tools read it.
func TestServiceStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local"), Generation: 42, Revision: "orders-00042-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	placed := time.Now()
	cmd := &ordersgen.PlaceOrder{CustomerId: uuid.New(), Currency: "USD", Items: []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 500}}}
	cmd.Namespace, cmd.AggregateID = "acme", uuid.New()
	if err := cli.Dispatch(ctx, cmd); err != nil {
		t.Fatal(err)
	}

	// nothing is running: no leader, and every checkpointed runner is
	// behind the order's event, since about when it was placed
	st, err := cli.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Service != "orders" || st.Latest == 0 || st.Lease.Needed || st.Lease.Held || st.Lease.Here || st.Lease.HereGeneration != 42 || st.Lease.HereRevision != "orders-00042-abc" {
		t.Fatalf("before any instance runs: %+v", st)
	}
	if st.Behind == 0 || st.Timers.Pending != 1 || st.Timers.Overdue != 0 || st.Timers.OldestDue == nil {
		t.Errorf("behind %d, timers %+v; want runners behind and the auto-cancel timer pending", st.Behind, st.Timers)
	}
	for _, r := range st.Runners {
		if r.Kind == "process(bus)" {
			continue
		}
		if r.Lag == 0 || r.BehindSince == nil || r.BehindSince.Before(placed.Add(-5*time.Second)) || r.BehindSince.After(time.Now().Add(5*time.Second)) {
			t.Errorf("%s: lag %d, behind since %v; want it behind since the order was placed", r.Runner, r.Lag, r.BehindSince)
		}
	}

	// an instance runs: the runners catch up. orders has no process
	// reacting to its own events, so it needs no leader and has none
	if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			raw, _ := json.MarshalIndent(st, "", " ")
			t.Logf("last status: %s", raw)
		}
	})
	waitFor(t, ctx, "the runners caught up", func() bool {
		st, err = cli.Status(ctx)
		return err == nil && st.Behind == 0
	})
	if st.Lease.Needed || st.Lease.Held {
		t.Errorf("orders' lease = %+v; want none needed and none held", st.Lease)
	}

	// billing has one: before it runs the lease is needed and not held;
	// running, this instance leads as the build it is
	bill, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: billing.NewRegistry(), Keys: testKeys(t), Generation: 7, Revision: "billing-00007-xyz"})
	if err != nil {
		t.Fatal(err)
	}
	bst, err := bill.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bst.Lease.Needed || bst.Lease.Held || bst.Lease.Here {
		t.Errorf("billing's lease before it runs = %+v; want needed and not held", bst.Lease)
	}
	if err := bill.Start(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "billing leads", func() bool {
		bst, err = bill.Status(ctx)
		// held a moment before the leader has named its session
		return err == nil && bst.Lease.Held && bst.Lease.Here && bst.Lease.Generation != 0
	})
	if !bst.Lease.Here || bst.Lease.Generation != 7 || bst.Lease.Revision != "billing-00007-xyz" || bst.Lease.Yielding {
		t.Errorf("billing's lease = %+v; want this instance leading as generation 7", bst.Lease)
	}
	for _, r := range st.Runners {
		if r.Lag != 0 || r.BehindSince != nil || r.Stalled {
			t.Errorf("%s after catching up: %+v", r.Runner, r)
		}
	}

	// the gateway: All access reads it; a tenant's admin does not
	tokens := map[string]loomgql.Access{
		"god":   {All: true},
		"admin": {Namespaces: []string{"acme"}, Mutate: true, Roles: map[string]string{"acme": "owner"}},
	}
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli, bill},
		Status:   true,
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
	// onlyBehind keeps what needs attention: a runner one event behind
	// shows, and a caught-up service shows none
	ex, _ := loomgql.AsExecutor(gateway)
	behind := func() string {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", "Bearer god")
		r, err := ex.Authenticate(r)
		if err != nil {
			t.Fatal(err)
		}
		res := ex.Exec(r.Context(), `{ serviceStatus { service runners(onlyBehind: true) { runner lag } } }`, nil)
		if len(res.Errors) > 0 {
			t.Fatal(res.Errors)
		}
		raw, _ := json.Marshal(res.Data["serviceStatus"])
		return string(raw)
	}
	if got := behind(); got != `[{"runners":[],"service":"orders"},{"runners":[],"service":"billing"}]` {
		t.Errorf("runners behind, all caught up = %s", got)
	}

	agents, err := loommcp.New(loommcp.Config{Gateway: gateway})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(agents)
	defer srv.Close()
	call := func(token, tool string, args map[string]any) (string, bool) {
		t.Helper()
		c := sdk.NewClient(&sdk.Implementation{Name: "test"}, nil)
		cs, err := c.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL, HTTPClient: &http.Client{Transport: bearer{token}}, DisableStandaloneSSE: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return res.Content[0].(*sdk.TextContent).Text, res.IsError
	}
	if text, _ := call("god", "search", map[string]any{"query": "background work"}); !strings.Contains(text, `"name":"serviceStatus"`) || !strings.Contains(text, `"tool":"list"`) {
		t.Errorf("search as god = %s", text)
	}
	if text, _ := call("admin", "search", map[string]any{"query": "background work"}); strings.Contains(text, "serviceStatus") {
		t.Errorf("a tenant's admin is shown serviceStatus: %s", text)
	}
	read := map[string]any{"name": "serviceStatus", "fields": []any{"service", "behind", "stalled", "lease.needed", "lease.held", "lease.here", "lease.revision", "timers.pending", "runners.runner", "runners.lag"}}
	if text, refused := call("admin", "list", read); !refused || !strings.Contains(text, "access denied") {
		t.Errorf("a tenant's admin reading serviceStatus = %s", text)
	}
	text, refused := call("god", "list", read)
	if refused {
		t.Fatalf("god reading serviceStatus: %s", text)
	}
	var got []struct {
		Service string
		Behind  int
		Lease   struct {
			Needed, Held, Here bool
			Revision           string
		}
		Timers  struct{ Pending int }
		Runners []struct {
			Runner string
			Lag    int
		}
	}
	if err := json.Unmarshal([]byte(text), &got); err != nil || len(got) != 2 {
		t.Fatalf("serviceStatus = %s (%v)", text, err)
	}
	if s := got[0]; s.Service != "orders" || s.Behind != 0 || s.Lease.Needed || s.Lease.Held || s.Timers.Pending != 1 || len(s.Runners) == 0 {
		t.Errorf("orders' status = %+v", s)
	}
	if s := got[1]; s.Service != "billing" || !s.Lease.Needed || !s.Lease.Held || !s.Lease.Here || s.Lease.Revision != "billing-00007-xyz" {
		t.Errorf("billing's status = %+v", s)
	}
}
