package graphql

import (
	"context"
	"fmt"
	"net/http"

	gql "github.com/graphql-go/graphql"
)

// Result is one in-process execution's answer: the data GraphQL
// returned and its errors as messages, in the order GraphQL gave them.
type Result struct {
	Data   map[string]any
	Errors []string
}

// Executor runs operations against the composed schema in-process,
// under the same Auth, Policy and depth limit as the HTTP endpoint —
// what another surface over the same gateway (loom/mcp) is built on, so
// it answers to one set of rules rather than a copy of them. The
// handler New returns is one (AsExecutor).
type Executor interface {
	// Authenticate resolves the request's Access exactly as the HTTP
	// endpoint does and returns the request carrying it. An Access the
	// deployment's own middleware already set on the context stands. An
	// error means unauthenticated.
	Authenticate(r *http.Request) (*http.Request, error)
	// Exec runs one operation. ctx is an authenticated request's
	// context: with an Auth hook mounted, a context carrying no Access
	// is refused rather than run open.
	Exec(ctx context.Context, query string, variables map[string]any) Result
	// Introspect is the schema's shape — every type, field and argument
	// with its description. It runs no resolver and reads no data.
	Introspect() (map[string]any, error)
}

// AsExecutor is the gateway handler New returned, as an Executor.
func AsExecutor(h http.Handler) (Executor, bool) {
	g, ok := h.(*handler)
	return g, ok
}

func (h *handler) Authenticate(r *http.Request) (*http.Request, error) {
	if _, ok := AccessFrom(r.Context()); ok || h.auth == nil {
		return r, nil
	}
	access, err := h.auth(r)
	if err != nil {
		return nil, err
	}
	return r.WithContext(WithAccess(r.Context(), access)), nil
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
