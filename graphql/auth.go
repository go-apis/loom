package graphql

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// The gateway's authorization model. Authentication stays the
// deployment's job — parse your JWT, API key, or session however you
// like — but the gateway needs to know what the caller may touch, and
// that answer is an Access:
//
//	gateway, _ := graphql.New(graphql.Config{
//	    Services: ...,
//	    Auth: func(r *http.Request) (graphql.Access, error) {
//	        claims, err := verify(r.Header.Get("Authorization"))
//	        if err != nil {
//	            return graphql.Access{}, err // 401
//	        }
//	        if claims.Admin {
//	            return graphql.Access{All: true, Mutate: true}, nil
//	        }
//	        return graphql.Access{Namespaces: claims.Orgs, Mutate: true}, nil
//	    },
//	})
//
// With no Auth hook the gateway is open (every namespace, all
// mutations) — the pre-auth behavior, for internal mounts behind
// trusted middleware. Wrap Files and Streams with Protect(auth, h) to
// enforce the same Access on downloads and raw watches.

// Access is what one caller may do. The zero value may do nothing.
type Access struct {
	// Namespaces this caller may read — and write, when Mutate is set.
	Namespaces []string
	// All is god mode: every namespace, and list queries/subscriptions
	// may omit `namespace` to search across all of them.
	All bool
	// Mutate allows mutations (commands and upload sessions).
	Mutate bool
	// Mutations optionally narrows Mutate to specific mutation field
	// names as they appear in the schema (e.g. "placeOrder",
	// "createW9Upload"). Empty means every mutation.
	Mutations []string
	// Roles is the caller's role per namespace; the "*" key applies to
	// every namespace. Commands declared with @role in the schema demand
	// one of the declared roles in the mutation's target namespace.
	// A nil map opts out of the role model: @role-gated mutations then
	// pass only for All (god) access — a scoped caller with no roles is
	// refused, so adding @role to a schema fails closed until the Auth
	// hook grants roles.
	Roles map[string]string
	// Claims is an opaque slot for whatever the Auth hook parsed — its
	// JWT claims, API-key record, session — carried untouched to the
	// Policy hook's Decision. The built-in checks never read it.
	Claims any
}

// Auth resolves a request to its Access. Returning an error means
// unauthenticated: the request is rejected with 401 before execution.
type Auth func(r *http.Request) (Access, error)

type accessKey struct{}

// WithAccess attaches an Access to the context — for deployments that
// authenticate in their own middleware instead of the Auth hook. A
// context Access takes precedence over Config.Auth.
func WithAccess(ctx context.Context, a Access) context.Context {
	return context.WithValue(ctx, accessKey{}, a)
}

// AccessFrom returns the context's Access. ok is false when none is set
// (an open mount).
func AccessFrom(ctx context.Context) (Access, bool) {
	a, ok := ctx.Value(accessKey{}).(Access)
	return a, ok
}

func (a Access) allows(ns string) bool {
	if a.All {
		return true
	}
	for _, n := range a.Namespaces {
		if n == ns {
			return true
		}
	}
	return false
}

// roleFor is the caller's role in one namespace: the exact grant, or
// the "*" wildcard grant.
func (a Access) roleFor(ns string) string {
	if r, ok := a.Roles[ns]; ok {
		return r
	}
	return a.Roles["*"]
}

// Decision is one operation the gateway wants authorized — the input
// document a Policy rules over. Args is the operation's arguments
// verbatim (mutation input included): policy code is trusted like
// handler code, so @secret fields are not redacted here.
type Decision struct {
	// Kind is query, mutation, subscription, file, or stream.
	Kind string
	// Field is the GraphQL field name ("" on file/stream mounts).
	Field string
	// Namespace is the operation's target; "*" is the cross-namespace
	// list form.
	Namespace string
	// Args carries the mutation input or query arguments.
	Args map[string]any
	// Fields lists the operation's requested selection paths, dotted
	// through nested selections and joins ("id", "secretHash",
	// "orders.status") — the whole tree the query can reach, fragments
	// resolved. Empty on file/stream mounts. A policy that forbids a
	// field denies the operation outright: the caller re-shapes the
	// query, partial results never happen.
	Fields []string
	// Roles is the command's @role contract — nil for ungated commands
	// and for reads. Advisory input: the policy decides what it means.
	Roles []string
	// Access is what the Auth hook resolved; HasAccess false means an
	// open mount (no Auth hook and no WithAccess middleware).
	Access    Access
	HasAccess bool
}

// Policy replaces the built-in authorization when set on Config: every
// operation — reads, lists, subscriptions, mutations, uploads, file
// downloads, raw watches — resolves to one Decision and the policy
// answers it. Return nil to allow; the error message becomes the
// GraphQL error (or the mount's 403). DefaultPolicy is the built-in
// rule set, exported so a policy handles its special cases and
// delegates the rest:
//
//	Policy: func(ctx context.Context, d loomgraphql.Decision) error {
//	    if c, ok := d.Access.Claims.(*Claims); ok && c.Staff != "" {
//	        return nil                      // platform staff: everything
//	    }
//	    return loomgraphql.DefaultPolicy(d) // everyone else: stock rules
//	}
//
// The ctx is the request context, so a policy may load aggregates or
// entities through a loom client to answer relationship rules ("is
// this client tagged to the caller's org?") — the OPA shape, with Go
// as the rule language.
type Policy func(ctx context.Context, d Decision) error

// DefaultPolicy is the gateway's built-in rule set over a Decision:
// open mounts allow everything; namespace "*" needs All; Namespaces
// scope reads and writes; Mutate (narrowed by Mutations) gates
// mutations; @role contracts demand a matching Roles grant in the
// target namespace (All with no Roles map keeps god-mode semantics).
// Custom policies call it to inherit these rules.
func DefaultPolicy(d Decision) error {
	if !d.HasAccess {
		return nil
	}
	a := d.Access
	if d.Kind != "mutation" {
		if d.Namespace == AllNamespaces {
			if !a.All {
				return fmt.Errorf(`access denied: namespace "*" needs all-namespace access`)
			}
			return nil
		}
		if !a.allows(d.Namespace) {
			return fmt.Errorf("access denied: namespace %q", d.Namespace)
		}
		return nil
	}
	if !a.Mutate {
		return fmt.Errorf("access denied: read-only access")
	}
	if !a.allows(d.Namespace) {
		return fmt.Errorf("access denied: namespace %q", d.Namespace)
	}
	if len(a.Mutations) > 0 {
		allowed := false
		for _, m := range a.Mutations {
			if m == d.Field {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("access denied: mutation %q", d.Field)
		}
	}
	if len(d.Roles) > 0 {
		// god access with no role model keeps its pre-@role meaning:
		// every mutation. Any Roles map switches to strict role checks.
		if a.All && len(a.Roles) == 0 {
			return nil
		}
		have := a.roleFor(d.Namespace)
		for _, want := range d.Roles {
			if have == want {
				return nil
			}
		}
		return fmt.Errorf("access denied: mutation %q needs role %s in namespace %q", d.Field, strings.Join(d.Roles, " or "), d.Namespace)
	}
	return nil
}

type policyKey struct{}

func withPolicy(ctx context.Context, p Policy) context.Context {
	return context.WithValue(ctx, policyKey{}, p)
}

func policyFrom(ctx context.Context) Policy {
	p, _ := ctx.Value(policyKey{}).(Policy)
	return p
}

// decide completes a Decision with the context's Access and asks the
// context's Policy — or DefaultPolicy when none is mounted.
func decide(ctx context.Context, d Decision) error {
	d.Access, d.HasAccess = AccessFrom(ctx)
	if p := policyFrom(ctx); p != nil {
		return p(ctx, d)
	}
	return DefaultPolicy(d)
}

// Protect wraps a handler (Files, Streams) with the same Auth the
// gateway uses: unauthenticated requests get 401, everything else runs
// with the Access on its context, which Files and Streams enforce.
func Protect(auth Auth, h http.Handler) http.Handler {
	return ProtectWith(auth, nil, h)
}

// ProtectWith is Protect carrying the gateway's Policy too, so file
// downloads and raw watches answer to the same rules as the schema.
func ProtectWith(auth Auth, policy Policy, h http.Handler) http.Handler {
	if auth == nil && policy == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if auth != nil {
			access, err := auth(r)
			if err != nil {
				http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
				return
			}
			ctx = WithAccess(ctx, access)
		}
		if policy != nil {
			ctx = withPolicy(ctx, policy)
		}
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

// namespaceOfKey pulls the namespace out of a blob key
// (service/namespace/stream/upload/file) for Files enforcement.
func namespaceOfKey(key string) string {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}
