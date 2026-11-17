// Package mcp serves a loom gateway to agents over the Model Context
// Protocol: five tools that reach every command and every read the
// composed GraphQL schema has, and nothing it has not.
//
// The gateway graphql.New returns serves this at /mcp itself (see
// graphql.MCPOptions); New and Mount are for mounting it by hand:
//
//	gateway, _ := graphql.New(graphql.Config{Services: ..., Auth: ..., Policy: ...})
//	agents, _ := mcp.New(mcp.Config{Gateway: gateway, Name: "orders"})
//	mux.Handle("/graphql", gateway)
//	mux.Handle("/mcp", agents)
//
// The tools are the same for every schema — search, describe, dispatch,
// get, list — so a service with seven hundred commands gives an agent
// five tools, not seven hundred. An agent finds an operation by name
// (search), reads what it takes (describe), then calls it. search and
// describe show a caller only what it could run: each operation is
// probed through the gateway's own Policy (graphql.Executor.Allowed).
//
// Nothing here decides who may do what. Every call becomes one GraphQL
// operation run in-process through the gateway (graphql.Executor), so it
// meets the gateway's Auth, its Policy, its @role gates, its depth limit
// and its redaction exactly as an HTTP caller's does. A hand-written
// graphql.Field is reachable the same way as a generated one.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-apis/loom/internal/gateway"
)

// Config mounts the tools over one gateway.
type Config struct {
	// Gateway is the handler graphql.New returned.
	Gateway http.Handler
	// Name and Version are what the server tells a client it is.
	// Name defaults to "loom".
	Name    string
	Version string
	// SignIn, when set, tells a client with no usable token how to get
	// one: the 401 names the sign-in document (SignIn.Metadata), which
	// names the authorization servers. Mount serves both.
	SignIn *SignIn
	// Unauthenticated answers a request the gateway's Auth refused, in
	// place of the 401 SignIn (or, without one, a bare Bearer challenge)
	// gives.
	Unauthenticated func(w http.ResponseWriter, r *http.Request)
}

// New is the MCP endpoint for the gateway (streamable HTTP, stateless:
// each request is authenticated on its own and no session is kept).
func New(cfg Config) (http.Handler, error) {
	ex, ok := cfg.Gateway.(gateway.Executor)
	if !ok {
		return nil, fmt.Errorf("mcp: Gateway is not a handler graphql.New returned")
	}
	schema, err := ex.Introspect()
	if err != nil {
		return nil, err
	}
	cat, err := newCatalogue(schema)
	if err != nil {
		return nil, err
	}
	s := &server{ex: ex, cat: cat, impl: &sdk.Implementation{Name: cfg.Name, Version: cfg.Version}, refuse: cfg.Unauthenticated}
	if s.impl.Name == "" {
		s.impl.Name = "loom"
	}
	if s.refuse == nil {
		challenge := "Bearer"
		if cfg.SignIn != nil {
			if challenge, err = cfg.SignIn.Challenge(); err != nil {
				return nil, err
			}
		}
		s.refuse = func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("WWW-Authenticate", challenge)
			http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
		}
	}
	// a server per request, its tools closed over that request's
	// context: the Access the gateway resolved rides it into every call
	s.http = sdk.NewStreamableHTTPHandler(func(r *http.Request) *sdk.Server { return s.serverFor(r.Context()) },
		&sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return s, nil
}

type server struct {
	ex     gateway.Executor
	cat    *catalogue
	impl   *sdk.Implementation
	refuse func(w http.ResponseWriter, r *http.Request)
	http   http.Handler
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	authed, err := s.ex.Authenticate(r)
	if err != nil {
		s.refuse(w, r)
		return
	}
	s.http.ServeHTTP(w, authed)
}

type searchIn struct {
	Query string `json:"query" jsonschema:"words to look for in operation names and descriptions; all must match. Empty lists everything"`
	Kind  string `json:"kind,omitempty" jsonschema:"command (changes something) or read; omit for both"`
	Limit int    `json:"limit,omitempty" jsonschema:"how many to return, 20 unless given, 100 at most"`
}

type describeIn struct {
	Name string `json:"name" jsonschema:"an operation name from search, or a type name from an earlier describe"`
}

type dispatchIn struct {
	callIn
	Key string `json:"key,omitempty" jsonschema:"optional: a key of your own naming this one thing you mean to do. Send the same key if you send the command again (after a timeout, or not knowing whether it ran) and it will not run twice: the repeat answers status duplicate"`
}

type callIn struct {
	Name   string         `json:"name" jsonschema:"the operation's name, as search and describe give it"`
	Args   map[string]any `json:"args,omitempty" jsonschema:"the operation's arguments by name, as describe lists them"`
	Fields []string       `json:"fields,omitempty" jsonschema:"which fields of the result to return, dotted for nested ones (lines.sku); omit for every plain field"`
}

func (s *server) serverFor(ctx context.Context) *sdk.Server {
	srv := sdk.NewServer(s.impl, nil)
	yes, no := true, false
	readOnly := &sdk.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}
	sdk.AddTool(srv, &sdk.Tool{
		Name:        "search",
		Description: "Find operations by name or description. Start here: it answers each match's name, whether it is a command (changes something) or a read, and a one-line summary. Then describe the one you want.",
		Annotations: readOnly,
	}, func(_ context.Context, _ *sdk.CallToolRequest, in searchIn) (*sdk.CallToolResult, any, error) {
		return s.search(ctx, in), nil, nil
	})
	sdk.AddTool(srv, &sdk.Tool{
		Name:        "describe",
		Description: "What one operation takes and returns: its arguments with their types (input objects expanded, enums with their values) and the fields of its result. Also describes a type by name.",
		Annotations: readOnly,
	}, func(_ context.Context, _ *sdk.CallToolRequest, in describeIn) (*sdk.CallToolResult, any, error) {
		return s.describe(ctx, in), nil, nil
	})
	sdk.AddTool(srv, &sdk.Tool{
		Name:        "dispatch",
		Description: "Run a command: an operation that changes something. Refused when the caller may not run it, exactly as the API refuses it. Some commands do not run at once: they answer a plan of what they would do, with an id. Show the plan, then run it with the runPlan command (args: namespace, id).",
		Annotations: &sdk.ToolAnnotations{DestructiveHint: &yes, OpenWorldHint: &no},
	}, func(_ context.Context, _ *sdk.CallToolRequest, in dispatchIn) (*sdk.CallToolResult, any, error) {
		keyed := ctx
		if in.Key != "" {
			keyed = gateway.WithIdempotencyKey(ctx, in.Key)
		}
		return s.call(keyed, in.callIn, kindCommand, false), nil, nil
	})
	sdk.AddTool(srv, &sdk.Tool{
		Name:        "get",
		Description: "Run a read that answers one record.",
		Annotations: readOnly,
	}, func(_ context.Context, _ *sdk.CallToolRequest, in callIn) (*sdk.CallToolResult, any, error) {
		return s.call(ctx, in, kindRead, false), nil, nil
	})
	sdk.AddTool(srv, &sdk.Tool{
		Name:        "list",
		Description: "Run a read that answers a list of records.",
		Annotations: readOnly,
	}, func(_ context.Context, _ *sdk.CallToolRequest, in callIn) (*sdk.CallToolResult, any, error) {
		return s.call(ctx, in, kindRead, true), nil, nil
	})
	return srv
}

func (s *server) search(ctx context.Context, in searchIn) *sdk.CallToolResult {
	if in.Kind != "" && in.Kind != kindCommand && in.Kind != kindRead {
		return refused(fmt.Sprintf("kind is %q or %q, not %q", kindCommand, kindRead, in.Kind))
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 20
	}
	limit = min(limit, 100)
	found := []map[string]any{}
	visible := func(op *operation) bool { return s.ex.Allowed(ctx, op.name) }
	for _, op := range s.cat.search(in.Query, in.Kind, limit, visible) {
		hit := map[string]any{"name": op.name, "kind": op.kind, "tool": s.toolFor(op)}
		if op.desc != "" {
			hit["summary"] = firstLine(op.desc)
		}
		found = append(found, hit)
	}
	return answer(map[string]any{"operations": found})
}

func (s *server) describe(ctx context.Context, in describeIn) *sdk.CallToolResult {
	op := s.cat.ops[in.Name]
	// an operation the caller could never run is not there for it
	if op != nil && !s.ex.Allowed(ctx, op.name) {
		return refused(fmt.Sprintf("no operation or type is named %q; search for it", in.Name))
	}
	if op == nil {
		if t := s.cat.describeType(in.Name, 3, s.ex.Hidden(ctx, in.Name)); t != nil {
			return answer(t)
		}
		return refused(fmt.Sprintf("no operation or type is named %q; search for it", in.Name))
	}
	out := map[string]any{"name": op.name, "kind": op.kind, "tool": s.toolFor(op)}
	if op.desc != "" {
		out["description"] = op.desc
	}
	out["args"] = s.cat.describeFields(op.args, true, 3, nil)
	returns := map[string]any{"type": op.typ.text}
	if td := s.cat.types[op.typ.name]; td != nil && !s.cat.leaf(op.typ) {
		returns["fields"] = s.cat.describeFields(td.fields, false, 0, s.ex.Hidden(ctx, td.name))
	}
	out["returns"] = returns
	return answer(out)
}

// toolFor names the tool that calls op.
func (s *server) toolFor(op *operation) string {
	switch {
	case op.kind == kindCommand:
		return "dispatch"
	case op.typ.list:
		return "list"
	default:
		return "get"
	}
}

func (s *server) call(ctx context.Context, in callIn, kind string, wantList bool) *sdk.CallToolResult {
	op := s.cat.ops[in.Name]
	if op == nil {
		return refused(fmt.Sprintf("no operation is named %q; search for it", in.Name))
	}
	if tool := s.toolFor(op); op.kind != kind || (kind == kindRead && op.typ.list != wantList) {
		return refused(fmt.Sprintf("%s is a %s: call it with the %s tool", op.name, op.kind, tool))
	}
	v := &view{hidden: func(typ string) map[string]string { return s.ex.Hidden(ctx, typ) }}
	doc, err := s.cat.document(op, in.Args, in.Fields, v)
	if err != nil {
		return refused(err.Error())
	}
	res := s.ex.Exec(ctx, doc, in.Args)
	if len(res.Errors) > 0 {
		return refused(strings.Join(res.Errors, "\n"))
	}
	out := answer(res.Data[op.name])
	// the data is as asked; what was left out because it is not this
	// caller's to see is said beside it, so an absent field is never
	// read as an empty one
	if len(v.left) > 0 && !out.IsError {
		out.Content = append(out.Content, &sdk.TextContent{Text: "Left out because hidden from you: " + strings.Join(v.left, "; ")})
	}
	return out
}

func answer(v any) *sdk.CallToolResult {
	raw, err := json.Marshal(v)
	if err != nil {
		return refused(err.Error())
	}
	return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: string(raw)}}}
}

func refused(msg string) *sdk.CallToolResult {
	return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: msg}}}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}
