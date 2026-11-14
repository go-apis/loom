package e2e_test

import (
	"bytes"
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

// TestAHiddenFieldSaysItIsHidden: a field's @allow hides it from a
// caller who may still read the record. Over GraphQL the field is null
// and the response says so at its path, under a code, so "hidden" is
// never read as "empty"; the caller cannot filter or order by it
// either. Through the MCP tools it is marked in describe, left out by
// default with a note saying so, and refused by name.
func TestAHiddenFieldSaysItIsHidden(t *testing.T) {
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
	depot := uuid.NewString()
	tokens := map[string]loomgql.Access{
		"admin":   {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "admin"}},
		"courier": {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "courier", "depot": depot}},
		"god":     {All: true, Mutate: true},
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
	api := httptest.NewServer(gateway)
	defer api.Close()
	type gqlError struct {
		Message    string         `json:"message"`
		Path       []any          `json:"path"`
		Extensions map[string]any `json:"extensions"`
	}
	type gqlResponse struct {
		Data   map[string]any `json:"data"`
		Errors []gqlError     `json:"errors"`
	}
	post := func(token, query string, vars map[string]any) gqlResponse {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
		req, _ := http.NewRequest(http.MethodPost, api.URL, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out gqlResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	parcel := uuid.NewString()
	if res := post("admin", `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`,
		map[string]any{"in": map[string]any{"aggregateId": parcel, "namespace": "acme", "depotId": depot, "label": "box", "insuredCents": 125000}}); len(res.Errors) > 0 {
		t.Fatal(res.Errors)
	}
	get := `query($id: UUID!) { parcel(namespace: "acme", id: $id) { label insuredCents } }`

	// who it is for sees it, and so does access with no attribute model
	for _, token := range []string{"admin", "god"} {
		res := post(token, get, map[string]any{"id": parcel})
		if got := res.Data["parcel"].(map[string]any); len(res.Errors) != 0 || got["insuredCents"] != float64(125000) {
			t.Errorf("%s reading the parcel: %+v, errors %+v", token, got, res.Errors)
		}
	}

	// the courier reads its parcel; the field is null, and says why
	res := post("courier", get, map[string]any{"id": parcel})
	got, _ := res.Data["parcel"].(map[string]any)
	if got["label"] != "box" || got["insuredCents"] != nil {
		t.Fatalf("the courier's parcel = %+v; want its label and no insured value", got)
	}
	if len(res.Errors) != 1 || fmt.Sprint(res.Errors[0].Path) != "[parcel insuredCents]" || res.Errors[0].Extensions["code"] != loomgql.FieldHidden ||
		res.Errors[0].Message != "hidden: Parcel.insuredCents is shown when kind is admin" {
		t.Errorf("the courier's errors = %+v; want one, at the field, coded %s", res.Errors, loomgql.FieldHidden)
	}
	// not asking for it is no error at all
	if res := post("courier", `query($id: UUID!) { parcel(namespace: "acme", id: $id) { label } }`, map[string]any{"id": parcel}); len(res.Errors) != 0 {
		t.Errorf("the courier reading only what it may: %+v", res.Errors)
	}

	// nor may it read the value back through a list's filter or order
	list := `query($where: [FilterInput!], $order: String) { parcels(namespace: "acme", where: $where, order: $order) { label } }`
	for doing, vars := range map[string]map[string]any{
		"filter": {"where": []any{map[string]any{"field": "insured_cents", "op": "GTE", "value": "100000"}}},
		"order":  {"order": "-insured_cents"},
	} {
		if res := post("courier", list, vars); len(res.Errors) != 1 || !strings.Contains(res.Errors[0].Message, "cannot "+doing+" by insured_cents, which is hidden from this caller") {
			t.Errorf("the courier listing by %s on the hidden field: data %+v, errors %+v", doing, res.Data, res.Errors)
		}
		if res := post("admin", list, vars); len(res.Errors) != 0 || len(res.Data["parcels"].([]any)) != 1 {
			t.Errorf("the admin listing by %s on it: data %+v, errors %+v", doing, res.Data, res.Errors)
		}
	}
	if res := post("courier", list, nil); len(res.Errors) != 0 || len(res.Data["parcels"].([]any)) != 1 {
		t.Errorf("the courier listing without it: %+v %+v", res.Data, res.Errors)
	}

	// through the MCP tools
	agents, err := loommcp.New(loommcp.Config{Gateway: gateway})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(agents)
	defer srv.Close()
	call := func(token, tool string, args map[string]any) (parts []string, refused bool) {
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
		for _, c := range res.Content {
			parts = append(parts, c.(*sdk.TextContent).Text)
		}
		return parts, res.IsError
	}
	read := map[string]any{"name": "parcel", "args": map[string]any{"namespace": "acme", "id": parcel}}

	// told before it asks
	parts, _ := call("courier", "describe", map[string]any{"name": "parcel"})
	if !strings.Contains(parts[0], `"hidden":"hidden from you: shown when kind is admin","name":"insuredCents"`) {
		t.Errorf("describe as the courier does not mark the field: %s", parts[0])
	}
	if parts, _ := call("admin", "describe", map[string]any{"name": "parcel"}); strings.Contains(parts[0], "hidden from you") {
		t.Errorf("describe as the admin marks a field hidden: %s", parts[0])
	}

	// left out by default, and said so beside the data
	parts, refused := call("courier", "get", read)
	if refused || len(parts) != 2 || strings.Contains(parts[0], "insuredCents") || !strings.Contains(parts[0], `"label":"box"`) ||
		parts[1] != "Left out because hidden from you: Parcel.insuredCents (shown when kind is admin)" {
		t.Errorf("the courier's get = %q (refused %v)", parts, refused)
	}
	parts, refused = call("courier", "list", map[string]any{"name": "parcels", "args": map[string]any{"namespace": "acme"}})
	if refused || len(parts) != 2 || strings.Contains(parts[0], "insuredCents") {
		t.Errorf("the courier's list = %q (refused %v)", parts, refused)
	}
	// asked for by name: refused, saying which and why
	read["fields"] = []any{"label", "insuredCents"}
	if parts, refused = call("courier", "get", read); !refused || !strings.Contains(parts[0], "Parcel.insuredCents is hidden from you: it is shown when kind is admin") {
		t.Errorf("the courier naming the hidden field = %q (refused %v)", parts, refused)
	}
	// the admin gets it, with nothing to note
	delete(read, "fields")
	if parts, refused = call("admin", "get", read); refused || len(parts) != 1 || !strings.Contains(parts[0], `"insuredCents":125000`) {
		t.Errorf("the admin's get = %q (refused %v)", parts, refused)
	}
}
