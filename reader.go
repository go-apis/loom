package loom

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// Handlers and reactions often need to read state — a process assembling
// an external call from several aggregates, a reaction filling a command
// from its own read model. The runtime injects read access into every
// handler invocation, so implementations call loom.Load/GetRecord/
// GetEntity on the ctx they were given instead of holding a client. This
// keeps handler structs to genuine external dependencies and kills the
// set-the-client-after-New wiring dance.
//
// Reads are deliberately the only capability injected: dispatching from
// inside a handler would nest units of work — return commands instead.

// StateReader is what handlers see behind loom.Load/GetRecord/GetEntity.
// The runtime injects the Client; tests inject fakes via WithStateReader.
type StateReader interface {
	Load(ctx context.Context, aggregate, namespace string, id uuid.UUID) (AggregateState, int, error)
	Record(ctx context.Context, record, namespace string, id uuid.UUID) (any, error)
	Entity(ctx context.Context, entity, namespace string, id uuid.UUID) (EntityState, error)
}

type readerKey struct{}

// WithStateReader injects read access for handler code under test.
func WithStateReader(ctx context.Context, r StateReader) context.Context {
	return context.WithValue(ctx, readerKey{}, r)
}

func withReader(ctx context.Context, c *Client) context.Context {
	return context.WithValue(ctx, readerKey{}, StateReader(c))
}

func reader(ctx context.Context) (StateReader, error) {
	c, ok := ctx.Value(readerKey{}).(StateReader)
	if !ok {
		return nil, fmt.Errorf("loom: no runtime in context — reads are available inside handlers and reactions")
	}
	return c, nil
}

// Load folds and returns an aggregate's current state and version, from
// inside a handler or reaction.
func Load(ctx context.Context, aggregate, namespace string, id uuid.UUID) (AggregateState, int, error) {
	c, err := reader(ctx)
	if err != nil {
		return nil, 0, err
	}
	return c.Load(ctx, aggregate, namespace, id)
}

// GetRecord reads a state-of-record row (nil if absent), from inside a
// handler or reaction.
func GetRecord(ctx context.Context, record, namespace string, id uuid.UUID) (any, error) {
	c, err := reader(ctx)
	if err != nil {
		return nil, err
	}
	return c.Record(ctx, record, namespace, id)
}

// GetEntity reads a read-model row (nil if absent), from inside a handler
// or reaction.
func GetEntity(ctx context.Context, entity, namespace string, id uuid.UUID) (EntityState, error) {
	c, err := reader(ctx)
	if err != nil {
		return nil, err
	}
	return c.Entity(ctx, entity, namespace, id)
}
