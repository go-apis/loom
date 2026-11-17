// Package gateway holds what the GraphQL gateway (loom/graphql) and the
// surfaces built over it (loom/mcp) share, so that the gateway can serve
// its MCP server itself without the two packages importing each other.
// loom/graphql re-exports every name here; nothing outside loom names
// this package.
package gateway

import (
	"context"
	"net/http"
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
// handler graphql.New returns is one (graphql.AsExecutor).
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
	// Allowed reports whether the caller on ctx could ever run the root
	// field named — a query or mutation of the composed schema — in any
	// namespace it has. It asks the same Policy a call would, as a probe
	// (Decision.Probe), so what a caller is shown follows the rules
	// that decide what it may do. It is a listing aid, not a grant: the
	// call itself is decided again with its arguments.
	Allowed(ctx context.Context, field string) bool
	// Hidden names the fields of the type named that are hidden from
	// the caller on ctx, each with what a caller must be to be shown it.
	// It follows the schema's per-field @allow rules, which compare the
	// caller alone, so it is the same for every record of the type.
	Hidden(ctx context.Context, typ string) map[string]string
	// Introspect is the schema's shape — every type, field and argument
	// with its description. It runs no resolver and reads no data.
	Introspect() (map[string]any, error)
}

type idempotencyKey struct{}

// WithIdempotencyKey puts a caller's idempotency key on ctx for the
// commands an Exec under it dispatches.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idempotencyKey{}, key)
}

// IdempotencyKey is the key WithIdempotencyKey put on ctx, if any.
func IdempotencyKey(ctx context.Context) string {
	key, _ := ctx.Value(idempotencyKey{}).(string)
	return key
}
