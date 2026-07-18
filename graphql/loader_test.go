package graphql

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
)

type countingReader struct {
	entityCalls int
	queryCalls  int
	state       loom.EntityState
	rows        []loom.Row
}

func (c *countingReader) Entity(ctx context.Context, entity, ns string, id uuid.UUID) (loom.EntityState, error) {
	c.entityCalls++
	return c.state, nil
}

func (c *countingReader) QueryEntities(ctx context.Context, entity string, q loom.Query) ([]loom.Row, error) {
	c.queryCalls++
	return c.rows, nil
}

type fakeState struct{ Name string }

func (f *fakeState) Fold(string, any) error { return nil }

func TestLoaderMemoizes(t *testing.T) {
	ctx := withLoader(context.Background())
	l := loaderFrom(ctx)
	if l == nil {
		t.Fatal("no loader in context")
	}
	r := &countingReader{state: &fakeState{Name: "x"}, rows: []loom.Row{{ID: "1"}}}
	id, other := uuid.New(), uuid.New()

	for i := 0; i < 3; i++ {
		if _, err := l.entity(ctx, r, "E", "ns", id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := l.entity(ctx, r, "E", "ns", other); err != nil {
		t.Fatal(err)
	}
	if r.entityCalls != 2 {
		t.Fatalf("entity reads: %d, want 2 (memoized per id)", r.entityCalls)
	}

	for i := 0; i < 3; i++ {
		if _, err := l.list(ctx, r, "E", "ns", "fk", "v"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := l.list(ctx, r, "E", "ns", "fk", "other"); err != nil {
		t.Fatal(err)
	}
	if r.queryCalls != 2 {
		t.Fatalf("list reads: %d, want 2 (memoized per key)", r.queryCalls)
	}

	// a fresh execution context gets a fresh memo
	if l2 := loaderFrom(withLoader(context.Background())); l2 == l {
		t.Fatal("loader leaked across contexts")
	}
	// no loader in a bare context — resolvers fall back to direct reads
	if loaderFrom(context.Background()) != nil {
		t.Fatal("expected nil loader from bare context")
	}
}
