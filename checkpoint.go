package loom

import (
	"context"
	"time"
)

// A runner writes its checkpoint only when it matters.
//
// Every projection and process reads the whole of its service's log and
// applies a few types of it. Passing the rest used to cost a checkpoint
// write per step, and a step per event once events came a few at a time:
// one runsheet e2e shard sent 4.07M statements for ~19.5k events, ~46
// checkpoint writes an event and 89% of everything Postgres did
// (2026-11-21, pg_stat_statements). skip.go had already made a long run
// of ignored events one write; this makes the trickle cheap too.
//
// A runner now holds its position in memory (runnerPos) and writes it:
//
//   - with every event it applies (a projection's fold, a process's
//     reaction), as before: what it applied is never re-applied;
//   - while passing events it does not subscribe to, at most once every
//     Config.CheckpointEvery (one second by default);
//   - when it goes idle with a position it has not written, once
//     CheckpointEvery has passed since its last write: runLogLoop wakes
//     it for that, however long its idle sleep.
//
// Parity. The row only ever holds a sequence S such that every
// subscribed event at or before S was applied (handled, parked or handed
// to a durable retry): the in-memory position runs ahead of the row only
// over events the runner does not subscribe to, because applying an
// event writes the row at it. A crash between writes restarts from the
// row and re-passes those events, which it would pass anyway; no handler
// runs twice.
//
// Every write is conditional on the row still holding what this runner
// last wrote or read (passCheckpoint): a rebuild, an operator's skip, a
// rewind, a reset or another instance's step that moved the row makes
// the write miss, and the runner reads the row again instead of
// overwriting it. A projection's fold commits only with that write, so a
// fold from a stale position rolls back. On this instance those movers
// also say so at once (runnerWake.resync); an idle runner re-reads its
// row at most every CheckpointEvery for the ones elsewhere.
//
// Liveness bound, for anything that reads loom_checkpoints to tell a
// moving runner from a stopped one (runsheet's platform watchdog, its
// /ops/runners, loom's own lag gauge): a running runner's row is at most
// CheckpointEvery (plus one step, and the wait for a step slot) behind
// the position it has reached, and it lags only over events the runner
// does not subscribe to. A row that has not reached the head several
// seconds after the log stopped moving is a runner that has stopped. A
// reader that needs a projection to have folded up to some sequence
// waits with AwaitProjection, which counts only the events the
// projection folds and so is not slowed by the bound.
//
// An idle step is free when the fan-out says there is nothing new
// (nothing to read, nothing to write), and one statement, with no
// transaction, when the runner reads its row (probeCheckpoint): its
// lock, its checkpoint and whether any event lies past it.

// checkpointEvery is Config.CheckpointEvery's default.
const checkpointEvery = time.Second

// runnerPos is one runner's position, held by its runLogLoop goroutine
// alone (steps run on it), so it needs no lock. rw.resync is the one
// field other goroutines touch, and it is atomic.
type runnerPos struct {
	runner string
	rw     *runnerWake
	every  time.Duration

	known     bool  // seq and persisted are trusted; false reads the row first
	seq       int64 // how far the runner has got
	persisted int64 // what the row holds, as this runner last wrote or read it
	exists    bool  // the row exists

	wroteAt time.Time // the last write of the row by this runner
	readAt  time.Time // the last time the row was read or written
}

func newRunnerPos(runner string, rw *runnerWake, every time.Duration) *runnerPos {
	if every <= 0 {
		every = checkpointEvery
	}
	return &runnerPos{runner: runner, rw: rw, every: every}
}

// begin starts a step: a resync asked for since the last one forgets the
// position, so the step reads the row.
func (p *runnerPos) begin() {
	if p.rw != nil && p.rw.resync.Swap(false) {
		p.known = false
	}
}

// forget makes the next step read the row: the runner is no longer sure
// where it is (an error, a write that missed, a lease or generation
// lost).
func (p *runnerPos) forget() { p.known = false }

// dirty is a position the row does not hold yet (or a row that does not
// exist: idle, but alive, is how runLogLoop tells a runner with nothing
// to do from one that never ran).
func (p *runnerPos) dirty() bool { return p.known && (p.seq != p.persisted || !p.exists) }

// readDue is whether the row should be read again: never read, or not
// looked at for CheckpointEvery.
func (p *runnerPos) readDue(now time.Time) bool {
	return !p.known || now.Sub(p.readAt) >= p.every
}

// writeDue is whether a dirty position may be written now.
func (p *runnerPos) writeDue(now time.Time) bool {
	return p.dirty() && (!p.exists || now.Sub(p.wroteAt) >= p.every)
}

// writeAt is when a dirty position falls due, for runLogLoop's sleep.
func (p *runnerPos) writeAt() time.Time {
	if !p.exists {
		return time.Time{}
	}
	return p.wroteAt.Add(p.every)
}

// adopt takes the row as read: a row other than the one this runner
// last knew (or a runner that knew none) moves the position to it.
func (p *runnerPos) adopt(seq int64, exists bool, now time.Time) {
	if !p.known || seq != p.persisted || exists != p.exists {
		p.seq = seq
	}
	p.persisted, p.exists, p.known, p.readAt = seq, exists, true, now
}

// wrote records a write of the row at seq.
func (p *runnerPos) wrote(seq int64, now time.Time) {
	p.seq, p.persisted, p.exists, p.known = seq, seq, true, true
	p.wroteAt, p.readAt = now, now
}

// probeCheckpoint is a runner's look at the store, in one statement and
// so with no transaction: whether its step lock is free (lock: a
// projection's; a process is elected by the lease instead), its row, and
// whether any event of the service lies past where it is. Where it is
// counts from the position held in memory while the row is still what
// the runner knew, else from the row.
//
// The transaction-scoped lock is taken and let go within the statement:
// false says another step (another instance's, or a Reset or SkipStalled
// in flight) holds it now, and the runner leaves the look to it.
func (c *Client) probeCheckpoint(ctx context.Context, q querier, pos *runnerPos, lock bool) (locked bool, seq int64, exists, more bool, err error) {
	knew := int64(-1) // a runner that knew no row counts from the row
	if pos.known {
		knew = pos.persisted
	}
	var row *int64
	err = q.QueryRow(ctx, `
		WITH c AS (SELECT global_seq FROM loom_checkpoints WHERE service=$1 AND runner=$2)
		SELECT
			CASE WHEN $5::bool THEN pg_try_advisory_xact_lock(hashtext('loom_' || $1 || '_' || $2)) ELSE true END,
			(SELECT global_seq FROM c),
			EXISTS (SELECT 1 FROM loom_events WHERE service=$1 AND global_seq > (
				CASE WHEN coalesce((SELECT global_seq FROM c), 0) = $4::bigint THEN $3::bigint
				ELSE coalesce((SELECT global_seq FROM c), 0) END))`,
		c.reg.Service, pos.runner, pos.seq, knew, lock).Scan(&locked, &row, &more)
	if err != nil {
		return false, 0, false, false, err
	}
	if row != nil {
		seq, exists = *row, true
	}
	return locked, seq, exists, more, nil
}

// lookCheckpoint reads the row when it is due (probeCheckpoint) and answers
// whether the step should go on to read events. A projection's look
// that finds its lock held, or any look that finds nothing past the
// runner's position, ends the step idle.
func (c *Client) lookCheckpoint(ctx context.Context, pos *runnerPos, lock bool, now time.Time) (bool, error) {
	if !pos.readDue(now) {
		return true, nil
	}
	if pos.writeDue(now) && pos.exists {
		// the write this step makes is conditional on the row, so it
		// is the look: a row that moved makes it miss (persist)
		return true, nil
	}
	locked, seq, exists, more, err := c.probeCheckpoint(ctx, c.db, pos, lock)
	if err != nil {
		pos.forget()
		return false, err
	}
	if !locked {
		return false, nil
	}
	pos.adopt(seq, exists, now)
	if !more {
		return false, c.idle(ctx, pos, now)
	}
	return true, nil
}

// idle ends a step with nothing to read: the position it holds is
// written once it falls due (and a runner with no row gets one at once).
func (c *Client) idle(ctx context.Context, pos *runnerPos, now time.Time) error {
	if !pos.writeDue(now) {
		return nil
	}
	_, err := c.persist(ctx, c.db, pos, pos.seq, now)
	return err
}

// passed moves the in-memory position over events the runner does not
// subscribe to, writing it when it falls due. false: the write missed,
// the row moved under the runner, which reads it again next step.
func (c *Client) passed(ctx context.Context, pos *runnerPos, to int64) (bool, error) {
	pos.seq = to
	now := time.Now()
	if !pos.writeDue(now) {
		return true, nil
	}
	return c.persist(ctx, c.db, pos, to, now)
}

// persist writes the row at seq, from where this runner last left it
// (passCheckpoint). A miss or an error forgets the position.
func (c *Client) persist(ctx context.Context, q executor, pos *runnerPos, seq int64, now time.Time) (bool, error) {
	moved, err := passCheckpoint(ctx, q, c.reg.Service, pos.runner, pos.persisted, seq)
	if err != nil || !moved {
		pos.forget()
		return false, err
	}
	pos.wrote(seq, now)
	return true, nil
}
