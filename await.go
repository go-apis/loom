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
func (c *Client) AwaitProjection(ctx context.Context, projection string, seq int64) error {
	runner := "projection:" + projection
	wait := 5 * time.Millisecond
	for {
		cur, err := c.readCheckpoint(ctx, runner)
		if err != nil {
			return err
		}
		if cur >= seq {
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
