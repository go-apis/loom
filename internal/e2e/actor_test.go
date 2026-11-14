package e2e_test

import (
	"context"
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

// TestTheGatewayRecordsWhoRanACommand: the actor the Auth hook names is
// on every event a caller's command emits — over GraphQL and through
// the MCP tools alike. A planned command's events name whoever ran the
// plan, and the plan as their cause.
func TestTheGatewayRecordsWhoRanACommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	admin := map[string]string{"kind": "admin"}
	tokens := map[string]loomgql.Access{
		"alice":   {Namespaces: []string{"acme"}, Mutate: true, Attrs: admin, Actor: "user:alice"},
		"planner": {Namespaces: []string{"acme"}, Mutate: true, Attrs: admin, Actor: "agent:planner"},
		"nobody":  {Namespaces: []string{"acme"}, Mutate: true, Attrs: admin},
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
	ex, _ := loomgql.AsExecutor(gateway)
	run := func(token, doc string, vars map[string]any) map[string]any {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r, err := ex.Authenticate(r)
		if err != nil {
			t.Fatal(err)
		}
		res := ex.Exec(r.Context(), doc, vars)
		if len(res.Errors) > 0 {
			t.Fatalf("%s: %v", token, res.Errors)
		}
		return res.Data
	}
	// last answers the newest event of a type on the parcel
	parcel := uuid.NewString()
	last := func(eventType string) loom.LogEntry {
		t.Helper()
		log, err := cli.QueryLog(ctx, loom.LogQuery{Namespace: "acme", AggregateID: parcel, Type: eventType, Limit: 1})
		if err != nil || len(log) != 1 {
			t.Fatalf("%s in the log: %+v, %v", eventType, log, err)
		}
		return log[0]
	}

	// over GraphQL
	run("alice", `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`,
		map[string]any{"in": map[string]any{"aggregateId": parcel, "namespace": "acme", "depotId": uuid.NewString(), "label": "box"}})
	if got := last("ParcelRegistered").Actor; got != "user:alice" {
		t.Errorf("ParcelRegistered's actor = %q, want user:alice", got)
	}

	// through the MCP tools
	agents, err := loommcp.New(loommcp.Config{Gateway: gateway})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(agents)
	defer srv.Close()
	c := sdk.NewClient(&sdk.Implementation{Name: "test"}, nil)
	cs, err := c.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL, HTTPClient: &http.Client{Transport: bearer{"planner"}}, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: "dispatch", Arguments: map[string]any{"name": "relabelParcel",
		"args": map[string]any{"input": map[string]any{"aggregateId": parcel, "namespace": "acme", "label": "crate"}}}})
	if err != nil || res.IsError {
		t.Fatalf("relabel through MCP: %v %+v", err, res)
	}
	if got := last("ParcelRelabelled").Actor; got != "agent:planner" {
		t.Errorf("ParcelRelabelled's actor = %q, want agent:planner", got)
	}

	// a plan made by one and run by another: the runner is the actor,
	// the plan the cause
	planned := run("planner", `mutation($in: DiscardParcelInput!) { discardParcel(input: $in) { id } }`,
		map[string]any{"in": map[string]any{"aggregateId": parcel, "namespace": "acme"}})
	planID := planned["discardParcel"].(map[string]any)["id"].(string)
	run("alice", `mutation($id: UUID!) { runPlan(namespace: "acme", id: $id) { status } }`, map[string]any{"id": planID})
	if got := last("ParcelDiscarded"); got.Actor != "user:alice" || got.CausationID != "plan:"+planID {
		t.Errorf("ParcelDiscarded: actor %q, cause %q; want user:alice and plan:%s", got.Actor, got.CausationID, planID)
	}

	// a caller the Auth hook names nobody for leaves the actor empty
	other := uuid.NewString()
	run("nobody", `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`,
		map[string]any{"in": map[string]any{"aggregateId": other, "namespace": "acme", "depotId": uuid.NewString(), "label": "box"}})
	log, err := cli.QueryLog(ctx, loom.LogQuery{Namespace: "acme", AggregateID: other, Limit: 1})
	if err != nil || len(log) != 1 || log[0].Actor != "" {
		t.Errorf("an unnamed caller's event: %+v, %v", log, err)
	}
}
