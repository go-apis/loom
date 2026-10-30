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
//
// Inside a unit of work (a command handler, or a policy reacting in the
// dispatch's transaction) the reads run on that transaction, never on the
// pool. The transaction holds the service's log lock from its first append
// to its commit, and every other dispatch waits on that lock holding a
// pooled connection of its own: a read that went to the pool from there
// waited for a connection those waiters held, and once they held every
// free one nothing moved (mesh-api, 2026-10-30). On the transaction a read
// also sees what the unit has written so far. A process reacts outside any
// transaction, so its reads use the pool as before.

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

// withTxReader injects reads that run on q, a unit of work's transaction.
func withTxReader(ctx context.Context, c *Client, q dbConn) context.Context {
	return context.WithValue(ctx, readerKey{}, StateReader(txReader{c: c, q: q}))
}

// txReader is a StateReader on one transaction (see withTxReader). A
// transaction serves one statement at a time, which a unit of work's
// handlers and policies already keep to: they run one after another.
type txReader struct {
	c *Client
	q dbConn
}

func (r txReader) Load(ctx context.Context, aggregate, namespace string, id uuid.UUID) (AggregateState, int, error) {
	return r.c.load(ctx, r.q, aggregate, namespace, id)
}

func (r txReader) Record(ctx context.Context, record, namespace string, id uuid.UUID) (any, error) {
	return r.c.record(ctx, r.q, record, namespace, id)
}

func (r txReader) Entity(ctx context.Context, entity, namespace string, id uuid.UUID) (EntityState, error) {
	return r.c.entity(ctx, r.q, entity, namespace, id)
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
