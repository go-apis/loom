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

// checkRead gates one namespace. An absent Access means an open mount.
func checkRead(ctx context.Context, ns string) error {
	a, ok := AccessFrom(ctx)
	if !ok || a.allows(ns) {
		return nil
	}
	return fmt.Errorf("access denied: namespace %q", ns)
}

// checkAll gates namespace: "*" — the cross-namespace list query.
func checkAll(ctx context.Context) error {
	a, ok := AccessFrom(ctx)
	if !ok || a.All {
		return nil
	}
	return fmt.Errorf(`access denied: namespace "*" needs all-namespace access`)
}

// roleFor is the caller's role in one namespace: the exact grant, or
// the "*" wildcard grant.
func (a Access) roleFor(ns string) string {
	if r, ok := a.Roles[ns]; ok {
		return r
	}
	return a.Roles["*"]
}

// checkMutate gates one mutation field into one namespace. roles is the
// command's @role contract — empty for ungated commands.
func checkMutate(ctx context.Context, field, ns string, roles []string) error {
	a, ok := AccessFrom(ctx)
	if !ok {
		return nil
	}
	if !a.Mutate {
		return fmt.Errorf("access denied: read-only access")
	}
	if !a.allows(ns) {
		return fmt.Errorf("access denied: namespace %q", ns)
	}
	if len(a.Mutations) > 0 {
		allowed := false
		for _, m := range a.Mutations {
			if m == field {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("access denied: mutation %q", field)
		}
	}
	if len(roles) > 0 {
		// god access with no role model keeps its pre-@role meaning:
		// every mutation. Any Roles map switches to strict role checks.
		if a.All && len(a.Roles) == 0 {
			return nil
		}
		have := a.roleFor(ns)
		for _, want := range roles {
			if have == want {
				return nil
			}
		}
		return fmt.Errorf("access denied: mutation %q needs role %s in namespace %q", field, strings.Join(roles, " or "), ns)
	}
	return nil
}

// Protect wraps a handler (Files, Streams) with the same Auth the
// gateway uses: unauthenticated requests get 401, everything else runs
// with the Access on its context, which Files and Streams enforce.
func Protect(auth Auth, h http.Handler) http.Handler {
	if auth == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		access, err := auth(r)
		if err != nil {
			http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r.WithContext(WithAccess(r.Context(), access)))
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
