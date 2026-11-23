package loom

import (
	"context"
	"slices"
	"sync"
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
// A runner now holds its position in memory (runnerPos), and its row is
// written:
//
//   - by the runner, with every event it applies (a projection's fold, a
//     process's reaction), as before: what it applied is never
//     re-applied; and once when it has no row yet;
//   - by the client's checkpoint flusher, for the position a runner has
//     reached by passing events it does not subscribe to: every
//     Config.CheckpointEvery (one second by default) it writes every
//     runner's unwritten position in one statement, so a runner's row
//     moves at most once a CheckpointEvery while it passes events, and
//     reaches where the runner stopped within one of it going idle. The
//     same statement reads every runner's row back.
//
// Parity. The row only ever holds a sequence S such that every
// subscribed event at or before S was applied (handled, parked or handed
// to a durable retry): the in-memory position runs ahead of the row only
// over events the runner does not subscribe to, because applying an
// event writes the row at it. A crash between writes restarts from the
// row and re-passes those events, which it would pass anyway; no handler
// runs twice.
//
// Every write is conditional on the row still holding what the runner
// last wrote or read (passCheckpoint, flushCheckpoints): a rebuild, an
// operator's skip, a rewind, a reset or another instance's step that
// moved the row makes the write miss, and the runner reads the row again
// instead of overwriting it. A projection's fold commits only with that
// write, so a fold from a stale position rolls back. On this instance
// those movers also say so at once (runnerWake.resync); the flusher's
// read of every row catches the ones elsewhere within a CheckpointEvery.
//
// Liveness bound, for anything that reads loom_checkpoints to tell a
// moving runner from a stopped one (runsheet's platform watchdog, its
// /ops/runners, loom's own lag gauge): a running runner's row is at most
// CheckpointEvery (plus the step it is in) behind the position it has
// reached, and it lags only over events the runner does not subscribe
// to. A row that has not reached the head a few seconds after the log
// stopped moving is a runner that has stopped. A reader that needs a
// runner to have applied everything up to some sequence asks
// AwaitProjection or RunnersBehind, which count only the events each
// runner subscribes to and so are not slowed by the bound.
//
// An idle step costs nothing when the runner knows where it is and the
// fan-out says there is nothing new, and one statement, with no
// transaction, when it does not know (probeCheckpoint): its lock, its
// checkpoint and whether any event lies past it.

// checkpointEvery is Config.CheckpointEvery's default.
const checkpointEvery = time.Second

// runnerPos is one runner's position. mu is held by the runner for the
// whole of each step and by the flusher while it writes and reads the
// row for it, so the two never write the row at once; the flusher only
// tries it, and leaves a runner in a step to the next flush. rw.resync
// is set by other goroutines, and is atomic.
type runnerPos struct {
	mu     sync.Mutex
	runner string
	rw     *runnerWake

	known     bool  // seq and persisted are trusted; false reads the row first
	seq       int64 // how far the runner has got
	persisted int64 // what the row holds, as this runner last wrote or read it
	exists    bool  // the row exists
}

func newRunnerPos(runner string, rw *runnerWake) *runnerPos {
	return &runnerPos{runner: runner, rw: rw}
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
// lost, a row somebody else moved).
func (p *runnerPos) forget() { p.known = false }

// dirty is a position the row does not hold yet.
func (p *runnerPos) dirty() bool { return p.known && p.exists && p.seq != p.persisted }

// adopt takes the row as read: a row other than the one this runner
// last knew (or a runner that knew none) moves the position to it.
func (p *runnerPos) adopt(seq int64, exists bool) {
	if !p.known || seq != p.persisted || exists != p.exists {
		p.seq = seq
	}
	p.persisted, p.exists, p.known = seq, exists, true
}

// wrote records a write of the row at seq.
func (p *runnerPos) wrote(seq int64) {
	p.seq, p.persisted, p.exists, p.known = seq, seq, true, true
}

// track registers a runner's position with the flusher.
func (c *Client) track(pos *runnerPos) *runnerPos {
	c.posMu.Lock()
	c.positions = append(c.positions, pos)
	c.posMu.Unlock()
	return pos
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

// lookCheckpoint reads the row when the runner does not know where it
// is (probeCheckpoint) and answers whether the step should go on to read
// events. A projection's look that finds its lock held, or any look that
// finds nothing past the runner's position, ends the step idle.
func (c *Client) lookCheckpoint(ctx context.Context, pos *runnerPos, lock bool) (bool, error) {
	if pos.known {
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
	pos.adopt(seq, exists)
	if !more {
		return false, c.idle(ctx, pos)
	}
	return true, nil
}

// idle ends a step with nothing to read. A runner with no row gets one
// now (idle, but alive, is how runLogLoop tells a runner with nothing to
// do from one that never ran); the flusher writes any other position.
func (c *Client) idle(ctx context.Context, pos *runnerPos) error {
	if !pos.known || pos.exists {
		return nil
	}
	_, err := c.persist(ctx, c.db, pos, pos.seq)
	return err
}

// passed moves the in-memory position over events the runner does not
// subscribe to; the flusher writes it.
func (c *Client) passed(ctx context.Context, pos *runnerPos, to int64) (bool, error) {
	pos.seq = to
	if !pos.exists {
		return c.persist(ctx, c.db, pos, to)
	}
	return true, nil
}

// persist writes the row at seq, from where this runner last left it
// (passCheckpoint). A miss or an error forgets the position.
func (c *Client) persist(ctx context.Context, q executor, pos *runnerPos, seq int64) (bool, error) {
	moved, err := passCheckpoint(ctx, q, c.reg.Service, pos.runner, pos.persisted, seq)
	if err != nil || !moved {
		pos.forget()
		return false, err
	}
	pos.wrote(seq)
	return true, nil
}

// runCheckpointFlusher flushes every CheckpointEvery until ctx ends.
func (c *Client) runCheckpointFlusher(ctx context.Context) {
	t := time.NewTicker(c.checkpointEvery)
	defer t.Stop()
	failed := logLimiter{every: time.Minute}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := c.flushCheckpoints(ctx); err != nil && ctx.Err() == nil {
			if count, ok := failed.allow(time.Now()); ok {
				c.log.ErrorContext(ctx, "checkpoint flush failed", "error", err, "failures", count)
			}
		}
	}
}

// flushCheckpoints writes the position of every runner not in a step
// that has passed events its row does not hold yet, and reads every
// runner's row back, in one statement. Each write moves the row only
// from where the runner last left it; a row that holds something else
// (moved under the runner, here or on another instance) has the runner
// read it again on its next step.
func (c *Client) flushCheckpoints(ctx context.Context) error {
	c.posMu.Lock()
	all := slices.Clone(c.positions)
	c.posMu.Unlock()
	var (
		held       []*runnerPos
		runners    []string
		froms, tos []int64
	)
	for _, p := range all {
		if !p.mu.TryLock() {
			continue // in a step: the next flush
		}
		if !p.known {
			p.mu.Unlock()
			continue
		}
		held = append(held, p)
		if p.dirty() {
			runners = append(runners, p.runner)
			froms = append(froms, p.persisted)
			tos = append(tos, p.seq)
		}
	}
	if len(held) == 0 {
		return nil
	}
	defer func() {
		for _, p := range held {
			p.mu.Unlock()
		}
	}()
	if runners == nil {
		runners, froms, tos = []string{}, []int64{}, []int64{}
	}
	// the rows are locked in runner order before they are written, and
	// a row somebody else holds (a step's transaction on another
	// instance, an operator's hand) is skipped, not waited for: a flush
	// holds every idle runner's position while it runs, so one held row
	// must not stall them all. A skipped row is written at the next
	// flush. The outer select reads the rows as they stood before the
	// update, so a written row reads its new value from the update's own
	// answer
	rows, err := c.db.Query(ctx, `
		WITH v AS (
			SELECT * FROM unnest($2::text[], $3::bigint[], $4::bigint[]) AS v(runner, from_seq, to_seq)
		), l AS (
			SELECT c.runner FROM loom_checkpoints c JOIN v ON v.runner = c.runner AND c.global_seq = v.from_seq
			WHERE c.service = $1
			ORDER BY c.runner
			FOR UPDATE OF c SKIP LOCKED
		), u AS (
			UPDATE loom_checkpoints c SET global_seq = v.to_seq, updated_at = now(),
				failing_seq = 0, attempts = 0, last_error = '', stalled_since = NULL
			FROM v, l
			WHERE c.service = $1 AND c.runner = v.runner AND l.runner = v.runner AND c.global_seq = v.from_seq
			RETURNING c.runner, c.global_seq
		)
		SELECT c.runner, coalesce(u.global_seq, c.global_seq)
		FROM loom_checkpoints c LEFT JOIN u ON u.runner = c.runner
		WHERE c.service = $1`,
		c.reg.Service, runners, froms, tos)
	if err != nil {
		return err
	}
	stored := map[string]int64{}
	for rows.Next() {
		var runner string
		var seq int64
		if err := rows.Scan(&runner, &seq); err != nil {
			rows.Close()
			return err
		}
		stored[runner] = seq
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, p := range held {
		seq, exists := stored[p.runner]
		switch {
		case exists && seq == p.seq:
			// written now, or already where the runner is
			p.wrote(seq)
		case exists && seq == p.persisted:
			// still what the runner knew
		case !exists && !p.exists:
		default:
			// moved under the runner: a rebuild, a skip, a rewind,
			// another instance; it reads the row again
			p.forget()
		}
	}
	return nil
}
