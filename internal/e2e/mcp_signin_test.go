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

	"github.com/modelcontextprotocol/go-sdk/oauthex"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
	loommcp "github.com/go-apis/loom/mcp"
)

// TestMCPSignInDiscovery: a client with no token is told where to read
// how to sign in, and that document names this server and the
// authorization servers whose tokens it takes — the two steps the MCP
// authorization spec has a client make before it asks for a token. A
// mount whose address is not the one the document names is refused.
func TestMCPSignInDiscovery(t *testing.T) {
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
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli},
		Auth: func(r *http.Request) (loomgql.Access, error) {
			if r.Header.Get("Authorization") != "Bearer good" {
				return loomgql.Access{}, fmt.Errorf("bad token")
			}
			return loomgql.Access{Namespaces: []string{"acme"}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	signIn := &loommcp.SignIn{Resource: srv.URL + "/agents/mcp", AuthorizationServers: []string{"https://auth.example.test"}, Scopes: []string{"orders"}}
	if err := loommcp.Mount(mux, "/agents/mcp", loommcp.Config{Gateway: gateway, SignIn: signIn}); err != nil {
		t.Fatal(err)
	}

	// no token, and a bad one: 401, naming the sign-in document
	for _, bearer := range []string{"", "Bearer wrong"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/agents/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", bearer)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		challenges, err := oauthex.ParseWWWAuthenticate(resp.Header.Values("WWW-Authenticate"))
		if resp.StatusCode != http.StatusUnauthorized || err != nil || len(challenges) != 1 {
			t.Fatalf("authorization %q: status %d, challenges %+v, %v", bearer, resp.StatusCode, challenges, err)
		}
		want := srv.URL + "/.well-known/oauth-protected-resource/agents/mcp"
		if c := challenges[0]; !strings.EqualFold(c.Scheme, "Bearer") || c.Params["resource_metadata"] != want || c.Params["scope"] != "orders" {
			t.Errorf("challenge = %+v; want Bearer naming %s", c, want)
		}

		// the document it names
		meta, err := http.Get(challenges[0].Params["resource_metadata"])
		if err != nil {
			t.Fatal(err)
		}
		var doc oauthex.ProtectedResourceMetadata
		err = json.NewDecoder(meta.Body).Decode(&doc)
		meta.Body.Close()
		if err != nil || meta.StatusCode != http.StatusOK || doc.Resource != signIn.Resource ||
			len(doc.AuthorizationServers) != 1 || doc.AuthorizationServers[0] != "https://auth.example.test" ||
			len(doc.ScopesSupported) != 1 || len(doc.BearerMethodsSupported) != 1 || doc.BearerMethodsSupported[0] != "header" {
			t.Errorf("sign-in document: status %d, %+v, %v", meta.StatusCode, doc, err)
		}
	}

	// a good token is served, not challenged
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/agents/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer good")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("a good token: status %d", resp.StatusCode)
	}

	// what cannot be mounted
	for what, bad := range map[string]loommcp.Config{
		"mounted somewhere else than it says": {Gateway: gateway, SignIn: &loommcp.SignIn{Resource: srv.URL + "/other", AuthorizationServers: []string{"https://auth.example.test"}}},
		"no authorization server":             {Gateway: gateway, SignIn: &loommcp.SignIn{Resource: srv.URL + "/x"}},
		"a relative resource":                 {Gateway: gateway, SignIn: &loommcp.SignIn{Resource: "/x", AuthorizationServers: []string{"https://auth.example.test"}}},
	} {
		if err := loommcp.Mount(http.NewServeMux(), "/x", bad); err == nil {
			t.Errorf("%s: mounted", what)
		}
	}
}
