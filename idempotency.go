package loom

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// A caller that does not hear back — a dropped connection, a timeout,
// an agent that lost its place — sends the command again. Most commands
// take that well: the handler sees the state the first one left and
// emits nothing, or refuses. Some do not: a command that appends, or
// one whose second run is a second charge. An idempotency key lets the
// caller say "this is the one I sent before": a dispatch under a key
// that has already committed is not run again.
//
// The key is recorded in the dispatch's own transaction, so it stands
// exactly when the dispatch did: one that failed left no key and runs
// when it is sent again, and two sent at once resolve to one. Keys are
// kept for IdempotencyWindow, which is how long "again" may be.

// IdempotencyWindow is how long a key is remembered.
const IdempotencyWindow = "7 days"

type idempotencyKey struct{}

type idempotency struct {
	key      string
	replayed bool
}

// WithIdempotencyKey marks ctx so that the next Dispatch under it runs
// only if no dispatch of the same command, to the same stream, has
// committed under key before. replayed reports, after that Dispatch
// returns nil, whether it was such a repeat and so did nothing.
//
// The key is the caller's: one per thing it means to do, however many
// times it sends it. It is scoped to the command and its stream, so one
// request that dispatches several commands may carry one key for all.
func WithIdempotencyKey(ctx context.Context, key string) (keyed context.Context, replayed func() bool) {
	if key == "" {
		return ctx, func() bool { return false }
	}
	i := &idempotency{key: key}
	return context.WithValue(ctx, idempotencyKey{}, i), func() bool { return i.replayed }
}

// claimIdempotency records the dispatch's key in its transaction and
// reports whether the dispatch should run. No key on ctx: it runs.
func (c *Client) claimIdempotency(ctx context.Context, tx pgx.Tx, cmds []Command) (run bool, err error) {
	i, ok := ctx.Value(idempotencyKey{}).(*idempotency)
	if !ok || len(cmds) == 0 {
		return true, nil
	}
	ns, id := cmds[0].CommandTarget()
	key := fmt.Sprintf("%s|%s|%s|%s", i.key, cmds[0].LoomCommand(), ns, id)
	if _, err := tx.Exec(ctx, `DELETE FROM loom_idempotency WHERE service=$1 AND at < now() - $2::interval`, c.reg.Service, IdempotencyWindow); err != nil {
		return false, err
	}
	// a concurrent dispatch under the same key holds this row until it
	// commits or rolls back: this one waits, and then runs or does not
	ct, err := tx.Exec(ctx, `
		INSERT INTO loom_idempotency (service, key) VALUES ($1,$2)
		ON CONFLICT (service, key) DO NOTHING`, c.reg.Service, key)
	if err != nil {
		return false, err
	}
	i.replayed = ct.RowsAffected() == 0
	return !i.replayed, nil
}
