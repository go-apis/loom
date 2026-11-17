package graphql

import (
	"context"
	"fmt"
	"net/http"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/gateway"
)

// Result is one in-process execution's answer: the data GraphQL
// returned and its errors as messages, in the order GraphQL gave them.
type Result = gateway.Result

// Executor runs operations against the composed schema in-process,
// under the same Auth, Policy and depth limit as the HTTP endpoint —
// what another surface over the same gateway (loom/mcp) is built on, so
// it answers to one set of rules rather than a copy of them. The
// handler New returns is one (AsExecutor). Its methods are documented
// on the shared definition the MCP server reads too.
type Executor = gateway.Executor

// AsExecutor is the gateway handler New returned, as an Executor.
func AsExecutor(h http.Handler) (Executor, bool) {
	g, ok := h.(*handler)
	return g, ok
}

func (h *handler) Authenticate(r *http.Request) (*http.Request, error) {
	ctx := r.Context()
	access, ok := AccessFrom(ctx)
	if !ok && h.auth != nil {
		var err error
		if access, err = h.auth(r); err != nil {
			return nil, err
		}
		ctx = WithAccess(ctx, access)
	}
	// a caller that may send a request twice names it once; an open
	// mount's callers too
	if key := r.Header.Get(IdempotencyHeader); key != "" {
		ctx = WithIdempotencyKey(ctx, key)
	}
	return r.WithContext(withActor(ctx, access)), nil
}

// IdempotencyHeader is the request header a caller names a request
// with: a command dispatched under a key that has already committed is
// not run again, and answers status "duplicate" (loom.WithIdempotencyKey).
const IdempotencyHeader = "Idempotency-Key"

// WithIdempotencyKey puts a caller's idempotency key on ctx for the
// commands an Exec under it dispatches — what the HTTP endpoint does
// with the Idempotency-Key header, for a surface that has no header.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return gateway.WithIdempotencyKey(ctx, key)
}

// keyed marks a dispatch's context with the request's idempotency key,
// if it has one, and reports afterwards whether the dispatch was a
// repeat that did nothing.
func keyed(ctx context.Context) (context.Context, func() bool) {
	return loom.WithIdempotencyKey(ctx, gateway.IdempotencyKey(ctx))
}

// dispatched is a command's answer: ok, or duplicate for a repeat of
// one that already ran under the caller's key.
func dispatched(replayed func() bool) map[string]any {
	if replayed() {
		return map[string]any{"status": "duplicate"}
	}
	return map[string]any{"status": "ok"}
}

// withActor names the caller on the metadata of whatever this request
// dispatches. Metadata that already names an actor stands: the
// deployment's middleware put it there.
func withActor(ctx context.Context, access Access) context.Context {
	meta := loom.MetaFrom(ctx)
	if access.Actor == "" || meta.Actor != "" {
		return ctx
	}
	meta.Actor = access.Actor
	return loom.WithMeta(ctx, meta)
}

func (h *handler) Exec(ctx context.Context, query string, variables map[string]any) Result {
	if _, ok := AccessFrom(ctx); !ok && h.auth != nil {
		return Result{Errors: []string{"unauthenticated"}}
	}
	if h.policy != nil {
		ctx = withPolicy(ctx, h.policy)
	}
	if h.maxDepth > 0 {
		if d := queryDepth(query); d > h.maxDepth {
			return Result{Errors: []string{fmt.Sprintf("query depth %d exceeds the limit of %d", d, h.maxDepth)}}
		}
	}
	return result(gql.Do(gql.Params{
		Schema:         h.schema,
		RequestString:  query,
		VariableValues: variables,
		Context:        withLoader(ctx),
	}))
}

func (h *handler) Allowed(ctx context.Context, field string) bool {
	a, has := AccessFrom(ctx)
	if !has {
		return h.auth == nil
	}
	g, ok := h.gates[field]
	if !ok {
		return false
	}
	if h.policy != nil {
		ctx = withPolicy(ctx, h.policy)
	}
	spaces := a.Namespaces
	switch {
	case !g.namespaced:
		spaces = []string{""}
	case a.All:
		spaces = []string{AllNamespaces}
	}
	for _, ns := range spaces {
		if decide(ctx, Decision{Kind: g.kind, Field: field, Namespace: ns, Roles: g.roles, Rules: g.rules, Probe: true}) == nil {
			return true
		}
	}
	return false
}

func (h *handler) Introspect() (map[string]any, error) {
	res := result(gql.Do(gql.Params{Schema: h.schema, RequestString: introspection, Context: context.Background()}))
	if len(res.Errors) > 0 {
		return nil, fmt.Errorf("graphql: introspection: %s", res.Errors[0])
	}
	schema, _ := res.Data["__schema"].(map[string]any)
	if schema == nil {
		return nil, fmt.Errorf("graphql: introspection answered no schema")
	}
	return schema, nil
}

func result(r *gql.Result) Result {
	out := Result{}
	out.Data, _ = r.Data.(map[string]any)
	for _, e := range r.Errors {
		out.Errors = append(out.Errors, e.Message)
	}
	return out
}

// introspection asks for what a caller needs to name and call every
// operation: types with their fields, arguments, input fields and enum
// values. Type references unwrap six levels, enough for [[T!]!]!.
const introspection = `{
  __schema {
    queryType { name }
    mutationType { name }
    types {
      kind name description
      fields { name description args { name description type { ...ref } } type { ...ref } }
      inputFields { name description type { ...ref } }
      enumValues { name }
    }
  }
}
fragment ref on __Type {
  kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } }
}`
