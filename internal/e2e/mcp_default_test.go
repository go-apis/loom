package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// mcpFixture is the orders service behind a gateway built with opts and
// nothing else wired: the gateway mounted at the root, as a service
// mounts it.
func mcpFixture(t *testing.T, ctx context.Context, opts loomgql.MCPOptions) *httptest.Server {
	t.Helper()
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
	tokens := map[string]loomgql.Access{
		"owner":  {Namespaces: []string{"acme"}, Mutate: true, Roles: map[string]string{"acme": "owner"}, Actor: "user:owner"},
		"member": {Namespaces: []string{"acme"}, Mutate: true, Roles: map[string]string{"acme": "member"}, Actor: "user:member"},
	}
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli},
		Issuer:   "https://auth.example.test",
		MCP:      opts,
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
	srv := httptest.NewServer(gateway)
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url, token, body string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if strings.HasSuffix(url, "/mcp") {
		req.Header.Set("Accept", "application/json, text/event-stream")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(raw)
}

// TestGatewayServesMCPByDefault: a gateway built with default options
// serves its MCP server at /mcp and the sign-in document naming its
// issuer, with no hand wiring; each call meets the gateway's own rules,
// so a member runs the command its role allows and is refused the one
// it does not, in the GraphQL mutation's own words.
func TestGatewayServesMCPByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := mcpFixture(t, ctx, loomgql.MCPOptions{})

	// no token: 401, naming the sign-in document beside /mcp
	resp, _ := post(t, srv.URL+"/mcp", "", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	challenges, err := oauthex.ParseWWWAuthenticate(resp.Header.Values("WWW-Authenticate"))
	want := srv.URL + "/.well-known/oauth-protected-resource/mcp"
	if resp.StatusCode != http.StatusUnauthorized || err != nil || len(challenges) != 1 || challenges[0].Params["resource_metadata"] != want {
		t.Fatalf("no token: status %d, challenges %+v, %v; want 401 naming %s", resp.StatusCode, challenges, err, want)
	}
	// the document names this endpoint and the issuer, at the RFC 9728
	// path and at the bare well-known path
	for _, at := range []string{want, srv.URL + "/.well-known/oauth-protected-resource"} {
		meta, err := http.Get(at)
		if err != nil {
			t.Fatal(err)
		}
		var doc oauthex.ProtectedResourceMetadata
		err = json.NewDecoder(meta.Body).Decode(&doc)
		meta.Body.Close()
		if err != nil || meta.StatusCode != http.StatusOK || doc.Resource != srv.URL+"/mcp" ||
			len(doc.AuthorizationServers) != 1 || doc.AuthorizationServers[0] != "https://auth.example.test" {
			t.Errorf("%s: status %d, %+v, %v", at, meta.StatusCode, doc, err)
		}
	}

	session := func(token string) *sdk.ClientSession {
		t.Helper()
		c := sdk.NewClient(&sdk.Implementation{Name: "test"}, nil)
		cs, err := c.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer{token}}, DisableStandaloneSSE: true}, nil)
		if err != nil {
			t.Fatalf("connect as %s: %v", token, err)
		}
		t.Cleanup(func() { cs.Close() })
		return cs
	}
	member := session("member")
	tools, err := member.ListTools(ctx, nil)
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
	if got := member.InitializeResult().ServerInfo.Name; got != "orders" {
		t.Errorf("server name = %q; want the service's", got)
	}

	dispatch := func(cs *sdk.ClientSession, args map[string]any) (string, bool) {
		t.Helper()
		res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: "dispatch", Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return res.Content[0].(*sdk.TextContent).Text, res.IsError
	}
	orderID := uuid.NewString()
	place := map[string]any{"name": "placeOrder", "args": map[string]any{"input": map[string]any{
		"aggregateId": orderID, "namespace": "acme", "customerId": uuid.NewString(), "currency": "USD",
		"items": []any{map[string]any{"sku": "widget", "quantity": 1, "priceCents": 100}},
	}}}
	if text, refused := dispatch(member, place); refused || !strings.Contains(text, `"status":"ok"`) {
		t.Fatalf("member placing an order = %s", text)
	}

	// shipOrder asks for @role(owner, shipper): the GraphQL mutation
	// refuses a member, and so does MCP, in the same words
	input := map[string]any{"aggregateId": orderID, "namespace": "acme"}
	body, _ := json.Marshal(map[string]any{
		"query":     `mutation($input: ShipOrderInput!) { shipOrder(input: $input) { status } }`,
		"variables": map[string]any{"input": input},
	})
	_, raw := post(t, srv.URL+"/graphql", "member", string(body))
	var gqlAnswer struct{ Errors []struct{ Message string } }
	if err := json.Unmarshal([]byte(raw), &gqlAnswer); err != nil || len(gqlAnswer.Errors) != 1 {
		t.Fatalf("GraphQL let a member ship, or answered %s", raw)
	}
	text, refused := dispatch(member, map[string]any{"name": "shipOrder", "args": map[string]any{"input": input}})
	if !refused || text != gqlAnswer.Errors[0].Message {
		t.Errorf("member shipping through MCP = %q (refused %v); GraphQL says %q", text, refused, gqlAnswer.Errors[0].Message)
	}
	// and the owner, whose role it asks for, ships it
	if text, refused := dispatch(session("owner"), map[string]any{"name": "shipOrder", "args": map[string]any{"input": input}}); refused || !strings.Contains(text, `"status":"ok"`) {
		t.Errorf("owner shipping = %s", text)
	}
}

// TestGatewayMCPOptOut: MCPOptions.Off serves no MCP server and no
// sign-in document; GraphQL is unchanged.
func TestGatewayMCPOptOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := mcpFixture(t, ctx, loomgql.MCPOptions{Off: true})

	if resp, _ := post(t, srv.URL+"/mcp", "owner", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`); resp.StatusCode != http.StatusNotFound {
		t.Errorf("/mcp with the opt-out: status %d; want 404", resp.StatusCode)
	}
	meta, err := http.Get(srv.URL + "/.well-known/oauth-protected-resource/mcp")
	if err != nil {
		t.Fatal(err)
	}
	meta.Body.Close()
	if meta.StatusCode != http.StatusNotFound {
		t.Errorf("sign-in document with the opt-out: status %d; want 404", meta.StatusCode)
	}
	if resp, raw := post(t, srv.URL+"/graphql", "owner", `{"query":"{ __typename }"}`); resp.StatusCode != http.StatusOK || !strings.Contains(raw, "Query") {
		t.Errorf("GraphQL with the opt-out: %d %s", resp.StatusCode, raw)
	}
}
