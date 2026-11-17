package mcp

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	sdkauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
)

// SignIn tells an MCP client how to sign in to this server: where the
// server is, and which authorization servers issue the tokens it takes
// (OAuth 2.0 Protected Resource Metadata, RFC 9728 — what the MCP
// authorization spec has a client read after a 401).
//
// It only points. loom issues no token and runs no sign-in: the
// authorization server does, and the gateway's Auth hook is still what
// reads the bearer and decides who it is — including that the token was
// issued for this Resource and not for some other server.
type SignIn struct {
	// Resource is this MCP endpoint's public URL, exactly as a client
	// is given it: https://api.example.com/mcp.
	Resource string
	// AuthorizationServers are the issuers whose tokens the gateway's
	// Auth hook accepts: https://auth.example.com.
	AuthorizationServers []string
	// Scopes are the scopes a client should ask for, if the
	// authorization server uses them.
	Scopes []string
}

// MetadataPath is where the sign-in document is served, on the same
// origin as Resource: /.well-known/oauth-protected-resource followed by
// Resource's own path, as RFC 9728 places it.
func (s SignIn) MetadataPath() (string, error) {
	u, err := s.resource()
	if err != nil {
		return "", err
	}
	return "/.well-known/oauth-protected-resource" + strings.TrimSuffix(u.Path, "/"), nil
}

// MetadataURL is MetadataPath on Resource's origin: what a 401 names.
func (s SignIn) MetadataURL() (string, error) {
	u, err := s.resource()
	if err != nil {
		return "", err
	}
	path, _ := s.MetadataPath()
	return u.Scheme + "://" + u.Host + path, nil
}

func (s SignIn) resource() (*url.URL, error) {
	u, err := url.Parse(s.Resource)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("mcp: SignIn.Resource %q is not an absolute URL", s.Resource)
	}
	if len(s.AuthorizationServers) == 0 {
		return nil, fmt.Errorf("mcp: SignIn names no authorization server")
	}
	return u, nil
}

// Metadata serves the sign-in document. Mount it at MetadataPath on
// the origin that serves the MCP endpoint (Mount does both).
func (s SignIn) Metadata() (http.Handler, error) {
	if _, err := s.resource(); err != nil {
		return nil, err
	}
	return sdkauth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
		Resource:               s.Resource,
		AuthorizationServers:   s.AuthorizationServers,
		ScopesSupported:        s.Scopes,
		BearerMethodsSupported: []string{"header"},
	}), nil
}

// Challenge is the WWW-Authenticate a request with no usable token is
// answered with: where to read how to sign in.
func (s SignIn) Challenge() (string, error) {
	meta, err := s.MetadataURL()
	if err != nil {
		return "", err
	}
	c := fmt.Sprintf(`Bearer resource_metadata=%q`, meta)
	if len(s.Scopes) > 0 {
		c += fmt.Sprintf(`, scope=%q`, strings.Join(s.Scopes, " "))
	}
	return c, nil
}

// Mount serves the gateway's MCP endpoint at path on mux and, when cfg
// has a SignIn, its sign-in document at the well-known path beside it.
// path must be the path of cfg.SignIn.Resource.
func Mount(mux *http.ServeMux, path string, cfg Config) error {
	h, err := New(cfg)
	if err != nil {
		return err
	}
	if cfg.SignIn != nil {
		u, err := cfg.SignIn.resource()
		if err != nil {
			return err
		}
		if strings.TrimSuffix(u.Path, "/") != strings.TrimSuffix(path, "/") {
			return fmt.Errorf("mcp: mounted at %q, but SignIn.Resource is %q: a client would be told to sign in for another address", path, cfg.SignIn.Resource)
		}
		meta, err := cfg.SignIn.Metadata()
		if err != nil {
			return err
		}
		at, _ := cfg.SignIn.MetadataPath()
		mux.Handle(at, meta)
	}
	mux.Handle(path, h)
	return nil
}
