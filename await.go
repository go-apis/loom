package loom

import (
	"context"
	"time"
)

// Read models are async: a request that dispatches and then reads a
// projection in the same breath — a sign-in that grants and then mints
// the token — races the runner, and usually wins by milliseconds. Head
// and AwaitProjection make that read-your-writes explicit: take the
// log head after the dispatch, wait for the projection's checkpoint to
// reach it (bound the wait with the ctx), then read.

// Head is the service's log head: the global sequence of its latest
// event, taken after a Dispatch so the caller knows what to wait for.
func (c *Client) Head(ctx context.Context) (int64, error) {
	return c.maxLogSeq(ctx)
}

// AwaitProjection returns once the named projection's checkpoint has
// reached seq — its rows reflect every event up to there — or when ctx
// ends. The runner may be on another instance; the checkpoint is polled
// with a short back-off, since checkpoint advances are not notified.
//
// Reached counts only the events the projection folds: a checkpoint
// short of seq with none of them between it and seq has folded all
// there is. A runner writes the position it reached by passing events
// it does not fold at most every Config.CheckpointEvery (checkpoint.go),
// so a wait on the checkpoint alone would pay up to that bound whenever
// the head was somebody else's event.
func (c *Client) AwaitProjection(ctx context.Context, projection string, seq int64) error {
	runner := "projection:" + projection
	var folds []string
	for _, p := range c.reg.Projections {
		if p.Name == projection {
			folds = p.Events
		}
	}
	wait := 5 * time.Millisecond
	for {
		var reached bool
		var err error
		if folds == nil {
			var cur int64
			cur, err = c.readCheckpoint(ctx, runner)
			reached = cur >= seq
		} else {
			reached, err = c.foldedThrough(ctx, runner, folds, seq)
		}
		if err != nil {
			return err
		}
		if reached {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		if wait < 100*time.Millisecond {
			wait *= 2
		}
	}
}

// foldedThrough reports whether a runner's checkpoint is at seq or past
// it, or short of it by events none of which are of the types it folds
// with seq already in the log (a sequence past the head is reached only
// by the checkpoint). A runner with no row has folded nothing.
func (c *Client) foldedThrough(ctx context.Context, runner string, folds []string, seq int64) (bool, error) {
	var reached bool
	err := c.db.QueryRow(ctx, `
		SELECT coalesce(bool_or(c.global_seq >= $3 OR (
			$3 <= (SELECT coalesce(max(e.global_seq), 0) FROM loom_events e WHERE e.service=$1)
			AND NOT EXISTS (
				SELECT 1 FROM loom_events e
				WHERE e.service=$1 AND e.global_seq > c.global_seq AND e.global_seq <= $3
					AND e.type = ANY($4::text[])))), false)
		FROM loom_checkpoints c WHERE c.service=$1 AND c.runner=$2`,
		c.reg.Service, runner, seq, folds).Scan(&reached)
	return reached, err
}
