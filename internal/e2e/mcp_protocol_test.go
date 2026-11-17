package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	loomgql "github.com/go-apis/loom/graphql"
)

// rpc is one JSON-RPC exchange with the MCP endpoint, as a raw client
// sends it: headers, the answer's headers and its decoded body.
type rpc struct {
	t     *testing.T
	url   string
	token string
}

func (c rpc) send(headers map[string]string, method string, params map[string]any) (http.Header, map[string]any) {
	c.t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, _ := http.NewRequest(http.MethodPost, c.url, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+c.token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.t.Fatalf("%s: status %d, %v", method, resp.StatusCode, err)
	}
	return resp.Header, out
}

// modern sends a request the 2026-07-28 way: no session, the protocol
// version and the client's capabilities in every request's _meta, the
// method (and tool) named in headers.
func (c rpc) modern(method, name string, params map[string]any) (http.Header, map[string]any) {
	c.t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{
		"io.modelcontextprotocol/protocolVersion":    "2026-07-28",
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
		"io.modelcontextprotocol/clientInfo":         map[string]any{"name": "raw", "version": "1"},
	}
	h := map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": method}
	if name != "" {
		h["Mcp-Name"] = name
	}
	return c.send(h, method, params)
}

func resultOf(t *testing.T, method string, msg map[string]any) map[string]any {
	t.Helper()
	res, ok := msg["result"].(map[string]any)
	if !ok {
		t.Fatalf("%s: no result: %v", method, msg)
	}
	return res
}

// TestMCPSpeaks20260728: the gateway's MCP server speaks MCP 2026-07-28
// — stateless, no initialize, no session; server/discover; every
// result typed complete and naming the server — and advertises only
// what it does: tools whose list never changes, and no logging. A
// client still on 2025-11-25, with initialize and Mcp-Session-Id, keeps
// working with the same tools, inputs and results.
func TestMCPSpeaks20260728(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := mcpFixture(t, ctx, loomgql.MCPOptions{})
	c := rpc{t: t, url: srv.URL + "/mcp", token: "owner"}

	// discover: the versions it speaks and what it does
	_, msg := c.modern("server/discover", "", nil)
	disc := resultOf(t, "server/discover", msg)
	if v, _ := json.Marshal(disc["supportedVersions"]); !strings.Contains(string(v), `"2026-07-28"`) || !strings.Contains(string(v), `"2025-11-25"`) {
		t.Errorf("supportedVersions = %s", v)
	}
	if caps, _ := json.Marshal(disc["capabilities"]); string(caps) != `{"tools":{}}` {
		t.Errorf("capabilities = %s; want tools with no list changes, and nothing else", caps)
	}

	// no initialize first, no session after
	h, msg := c.modern("tools/list", "", nil)
	list := resultOf(t, "tools/list", msg)
	if h.Get("Mcp-Session-Id") != "" {
		t.Errorf("a stateless server named a session: %q", h.Get("Mcp-Session-Id"))
	}
	var names []string
	for _, tool := range list["tools"].([]any) {
		names = append(names, tool.(map[string]any)["name"].(string))
	}
	if got := strings.Join(names, " "); got != "describe dispatch get list search" || list["resultType"] != "complete" {
		t.Errorf("tools/list = %q, resultType %v", got, list["resultType"])
	}
	if _, ok := list["ttlMs"]; !ok || list["cacheScope"] == nil {
		t.Errorf("tools/list carries no cache fields: %v", list)
	}

	// a command and a read, each in one request
	orderID := uuid.NewString()
	_, msg = c.modern("tools/call", "dispatch", map[string]any{"name": "dispatch", "arguments": map[string]any{"name": "placeOrder", "args": map[string]any{"input": map[string]any{
		"aggregateId": orderID, "namespace": "acme", "customerId": uuid.NewString(), "currency": "USD",
		"items": []any{map[string]any{"sku": "widget", "quantity": 1, "priceCents": 100}},
	}}}})
	res := resultOf(t, "dispatch", msg)
	if res["resultType"] != "complete" || res["isError"] == true || !strings.Contains(textOf(res), `"status":"ok"`) {
		t.Fatalf("dispatch = %v", res)
	}
	if info, _ := res["_meta"].(map[string]any)["io.modelcontextprotocol/serverInfo"].(map[string]any); info["name"] != "orders" {
		t.Errorf("result names no server: %v", res["_meta"])
	}
	_, msg = c.modern("tools/call", "get", map[string]any{"name": "get", "arguments": map[string]any{"name": "order", "args": map[string]any{"namespace": "acme", "id": orderID}, "fields": []any{"status"}}})
	if text := textOf(resultOf(t, "get", msg)); text != `{"status":"placed"}` {
		t.Errorf("get = %s", text)
	}

	// a request that names no protocol version in _meta is refused
	_, msg = c.send(map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/list"}, "tools/list", map[string]any{})
	if msg["error"] == nil {
		t.Errorf("a 2026-07-28 request with no _meta version was served: %v", msg)
	}

	// 2025-11-25, by hand: initialize, then a call under whatever
	// session the client holds
	h, msg = c.send(nil, "initialize", map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "old", "version": "1"}})
	if init := resultOf(t, "initialize", msg); init["protocolVersion"] != "2025-11-25" {
		t.Fatalf("initialize = %v", init)
	}
	session := h.Get("Mcp-Session-Id")
	if session == "" {
		session = "a-session-the-client-kept"
	}
	_, msg = c.send(map[string]string{"Mcp-Session-Id": session, "MCP-Protocol-Version": "2025-11-25"}, "tools/call", map[string]any{"name": "get", "arguments": map[string]any{"name": "order", "args": map[string]any{"namespace": "acme", "id": orderID}, "fields": []any{"status"}}})
	if text := textOf(resultOf(t, "legacy get", msg)); text != `{"status":"placed"}` {
		t.Errorf("legacy get = %s", text)
	}

	// and the SDK's client, held to 2025-11-25
	old := sdk.NewClient(&sdk.Implementation{Name: "old"}, nil)
	cs, err := old.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: c.url, HTTPClient: &http.Client{Transport: bearer{"owner"}}, DisableStandaloneSSE: true}, &sdk.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	if got := cs.InitializeResult().ProtocolVersion; got != "2025-11-25" {
		t.Errorf("legacy client negotiated %q", got)
	}
	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 5 {
		t.Fatalf("legacy tools/list = %v, %v", tools, err)
	}
	got, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: "search", Arguments: map[string]any{"query": "place order"}})
	if err != nil || got.IsError || !strings.Contains(got.Content[0].(*sdk.TextContent).Text, `"name":"placeOrder"`) {
		t.Errorf("legacy search = %v, %v", got, err)
	}
}

func textOf(res map[string]any) string {
	content, _ := res["content"].([]any)
	if len(content) == 0 {
		return ""
	}
	text, _ := content[0].(map[string]any)["text"].(string)
	return text
}
