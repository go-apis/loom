package loom

import (
	"context"
	"sync/atomic"
	"time"
)

// Local-event processes are elected per service: one instance leads and
// its process runners step; the rest yield. The lease is a session-scoped
// advisory lock held on a dedicated connection for as long as this
// instance leads — Postgres releases it the moment that session ends, so
// a crashed or scaled-down leader is replaced at the followers' next try
// (one poll interval). A projection's exclusion is per step, in the
// transaction its folds already use; a process reacts outside any
// transaction — an external call may take seconds — so its exclusion
// must not hold a pool connection through the reaction, and one lease
// per instance costs one connection, only while leading.
//
// Across a leadership change the outgoing leader may still be finishing
// the event it was on when the incoming one starts: at-least-once, the
// guarantee processes carry anyway (effects are journaled, commands
// converge, redeliveries dedup). In steady state exactly one instance
// reacts.

// Leading reports whether this instance holds the service's process
// lease — the one whose local-event processes are reacting. Exposed for
// health pages and tests; nothing in the runtime needs it outside the
// process runners.
func (c *Client) Leading() bool { return c.leader.Load() }

func (c *Client) leading() bool { return c.Leading() }

// runElection keeps trying for the lease; while held, keeps the
// connection alive and watches for its loss.
func (c *Client) runElection(ctx context.Context, poll time.Duration) {
	for ctx.Err() == nil {
		held, err := c.lead(ctx, poll)
		if err != nil && ctx.Err() == nil {
			c.log.WarnContext(ctx, "process election", "error", err)
		}
		if held {
			c.log.InfoContext(ctx, "process lease released")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(poll):
		}
	}
}

// lead tries the lease once. A follower returns at once, holding nothing.
// A leader keeps the connection until ctx ends or the connection fails,
// and reports true so the caller can log the hand-back.
func (c *Client) lead(ctx context.Context, poll time.Duration) (held bool, err error) {
	conn, err := c.db.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('loom_processes_' || $1))`, c.reg.Service).Scan(&got); err != nil {
		return false, err
	}
	if !got {
		return false, nil
	}
	c.leader.Store(true)
	defer c.leader.Store(false)
	c.log.InfoContext(ctx, "process lease acquired")
	// the runners may be asleep on their tick: the lease is new work
	c.fan.wakeAll()

	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// hand the lease back deliberately rather than waiting for the
			// session to die with the process; best-effort, the pool will
			// close the connection either way
			release, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = conn.Exec(release, `SELECT pg_advisory_unlock(hashtext('loom_processes_' || $1))`, c.reg.Service)
			cancel()
			return true, nil
		case <-t.C:
			// the lease lives with the session: a connection that no longer
			// answers has lost it, whatever this instance believes
			if _, err := conn.Exec(ctx, `SELECT 1`); err != nil {
				return true, err
			}
		}
	}
}

// leaderFlag is the process lease as this instance sees it.
type leaderFlag struct{ atomic.Bool }
