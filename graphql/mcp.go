package graphql

import (
	"net/http"
	"strings"

	"github.com/go-apis/loom/mcp"
)

// The gateway serves its MCP server itself. A request whose path ends
// in /mcp reaches the generated MCP server (package mcp: search,
// describe, dispatch, get, list), each call run as one operation of
// this gateway under the caller's Auth, Policy, @role and @allow gates;
// a request under /.well-known/oauth-protected-resource reads the
// sign-in document naming Config.Issuer (RFC 9728). Every other path is
// GraphQL, as before. Mount the gateway at the root (or a prefix) for
// /mcp to reach it: mux.Handle("/", gateway).

// MCPPath is where the gateway serves its MCP server.
const MCPPath = "/mcp"

const wellKnownResource = "/.well-known/oauth-protected-resource"

// MCPOptions shapes the MCP server the gateway serves at /mcp.
type MCPOptions struct {
	// Off opts out: the gateway serves no MCP server, and /mcp and the
	// sign-in document answer 404.
	Off bool
	// Name and Version are what the server tells a client it is. Name
	// defaults to the first composed service's name.
	Name    string
	Version string
	// Resource is the endpoint's public URL, when it is not the
	// request's own origin and path — behind a proxy that rewrites
	// either: https://api.example.com/mcp.
	Resource string
	// Scopes are the scopes a client should ask the Issuer for, if it
	// uses them.
	Scopes []string
}

type agents struct {
	mcp    http.Handler
	issuer string
	opts   MCPOptions
}

func (h *handler) mountMCP(cfg Config) error {
	if cfg.MCP.Off {
		return nil
	}
	a := &agents{issuer: cfg.Issuer, opts: cfg.MCP}
	name := cfg.MCP.Name
	if name == "" {
		name = cfg.Services[0].Registry().Service
	}
	srv, err := mcp.New(mcp.Config{Gateway: h, Name: name, Version: cfg.MCP.Version, Unauthenticated: a.refuse})
	if err != nil {
		return err
	}
	a.mcp = srv
	h.agents = a
	return nil
}

// serveMCP answers the MCP endpoint and its sign-in document, and
// reports whether r was one of them.
func (h *handler) serveMCP(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	switch {
	case strings.Contains(path, wellKnownResource):
		if h.agents == nil || h.agents.issuer == "" {
			http.NotFound(w, r)
			return true
		}
		_, at, _ := strings.Cut(path, wellKnownResource)
		if at == "" {
			at = MCPPath
		}
		meta, err := h.agents.signIn(r, at).Metadata()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return true
		}
		meta.ServeHTTP(w, r)
		return true
	case strings.HasSuffix(path, MCPPath):
		if h.agents == nil {
			http.NotFound(w, r)
			return true
		}
		h.agents.mcp.ServeHTTP(w, r)
		return true
	}
	return false
}

// signIn is the sign-in document for the MCP endpoint at path on r's
// origin, or at MCPOptions.Resource.
func (a *agents) signIn(r *http.Request, path string) mcp.SignIn {
	resource := a.opts.Resource
	if resource == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
			scheme, _, _ = strings.Cut(p, ",")
		}
		resource = strings.TrimSpace(scheme) + "://" + r.Host + path
	}
	return mcp.SignIn{Resource: resource, AuthorizationServers: []string{a.issuer}, Scopes: a.opts.Scopes}
}

// refuse answers an MCP request the gateway's Auth refused: 401, and
// with an Issuer, a challenge naming the sign-in document.
func (a *agents) refuse(w http.ResponseWriter, r *http.Request) {
	challenge := "Bearer"
	if a.issuer != "" {
		if c, err := a.signIn(r, r.URL.Path).Challenge(); err == nil {
			challenge = c
		}
	}
	w.Header().Set("WWW-Authenticate", challenge)
	http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
}
