package graphql

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
)

// entityReader is what the join loader needs from a service client —
// an interface so tests can count calls.
type entityReader interface {
	Entity(ctx context.Context, entity, namespace string, id uuid.UUID) (loom.EntityState, error)
	QueryEntities(ctx context.Context, entity string, q loom.Query) ([]loom.Row, error)
}

// loader memoizes join lookups within one query execution: cyclic
// declared joins and lists sharing a foreign key would otherwise
// re-read the same rows once per occurrence. It lives exactly one
// gql.Do call — never in a subscription context, where a memo would
// serve stale rows across ticks. Errors are not memoized, so a
// transient failure on one field doesn't poison a later retry.
type loader struct {
	mu      sync.Mutex
	singles map[singleKey]loom.EntityState
	lists   map[listKey][]loom.Row
}

type singleKey struct {
	reader entityReader
	entity string
	ns     string
	id     uuid.UUID
}

type listKey struct {
	reader entityReader
	entity string
	ns     string
	via    string
	value  string
}

type loaderCtxKey struct{}

func withLoader(ctx context.Context) context.Context {
	return context.WithValue(ctx, loaderCtxKey{}, &loader{
		singles: map[singleKey]loom.EntityState{},
		lists:   map[listKey][]loom.Row{},
	})
}

func loaderFrom(ctx context.Context) *loader {
	l, _ := ctx.Value(loaderCtxKey{}).(*loader)
	return l
}

// entity reads one row through the memo; without a loader in context it
// falls through to a direct read.
func (l *loader) entity(ctx context.Context, r entityReader, entity, ns string, id uuid.UUID) (loom.EntityState, error) {
	key := singleKey{r, entity, ns, id}
	l.mu.Lock()
	if state, ok := l.singles[key]; ok {
		l.mu.Unlock()
		return state, nil
	}
	l.mu.Unlock()
	state, err := r.Entity(ctx, entity, ns, id)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.singles[key] = state
	l.mu.Unlock()
	return state, nil
}

// list reads a filtered child list through the memo.
func (l *loader) list(ctx context.Context, r entityReader, entity, ns, via, value string) ([]loom.Row, error) {
	key := listKey{r, entity, ns, via, value}
	l.mu.Lock()
	if rows, ok := l.lists[key]; ok {
		l.mu.Unlock()
		return rows, nil
	}
	l.mu.Unlock()
	rows, err := r.QueryEntities(ctx, entity, loom.Query{
		Namespace: ns,
		Filters:   []loom.Filter{{Field: via, Value: value}},
	})
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.lists[key] = rows
	l.mu.Unlock()
	return rows, nil
}
