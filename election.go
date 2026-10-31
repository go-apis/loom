package loom

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
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
//
// # A newer generation takes the lease (ADR 0010)
//
// A session lock knows nothing of builds: an instance of the previous
// revision that is still alive after a deploy (Cloud Run keeps it for a
// traffic tag, idles it with CPU always allocated, or lets a deleted
// revision run on for minutes) keeps the lease, and the processes run the
// old code and configuration until its session ends. So an instance with
// a generation (Config.Generation, by default the sequence of Cloud Run's
// K_REVISION) names it on its lease session (application_name
// "loom lease <generation> <revision>"), and a follower of a strictly
// newer generation takes the lease over:
//
//  1. It finds the leader's session (pg_locks joined to pg_stat_activity)
//     and reads its generation. An equal or newer one, or a session that
//     names none (an older loom, an instance with no generation), is left
//     alone: an older generation never takes the lease from a newer one.
//  2. It asks the leader to yield: NOTIFY on loom.process_lease. The
//     leader LISTENs on its lease session, so it hears at once. It marks
//     its session yielding, stops its process runners taking another
//     event, waits for the reaction in flight to checkpoint (at most
//     handoverDrain), releases the lock, and stays out of the election
//     for two polls.
//  3. Meanwhile the newer follower tries the lock every fastPoll, so it
//     leads within milliseconds of the release.
//  4. A leader that does not answer — a CPU-throttled or wedged instance
//     — is not waited for: if its session is not marked yielding within
//     two polls (or is still held handoverDrain after it was), the newer
//     follower ends it (pg_terminate_backend). That is a crash as far as
//     the processes are concerned, the at-least-once hand-over above.
//
// With no generation (Config.Generation unset and no K_REVISION, or
// negative) nothing of this happens: the election is as it always was.

// Leading reports whether this instance holds the service's process
// lease — the one whose local-event processes are reacting. Exposed for
// health pages and tests; nothing in the runtime needs it outside the
// process runners.
func (c *Client) Leading() bool { return c.leader.Load() }

func (c *Client) leading() bool { return c.Leading() }

// leaseChannel carries "<service> <generation>" from a newer follower to
// the leader it asks to yield. The dot keeps it apart from the per-service
// "loom_<service>" log channels.
const leaseChannel = "loom.process_lease"

// handoverDrain bounds how long a yielding leader waits for the reaction
// in flight before it releases the lease anyway, and so how long a newer
// follower waits on a leader that said it is yielding before it ends the
// leader's session.
var handoverDrain = 30 * time.Second

// generation is this instance's build as the process election orders it.
type generation struct {
	n        int64  // 0: none — the election ignores builds
	revision string // a name for logs and the lease session; may be empty
}

// generationFrom resolves Config.Generation and Config.Revision. Revision
// defaults to K_REVISION; Generation defaults to the revision's Cloud Run
// sequence; a negative Generation turns generations off.
func generationFrom(n int64, revision string) generation {
	if revision == "" {
		revision = os.Getenv("K_REVISION")
	}
	if n < 0 {
		return generation{revision: revision}
	}
	if n == 0 {
		n, _ = RevisionSequence(revision)
	}
	return generation{n: n, revision: revision}
}

// RevisionSequence is the number Cloud Run gives a revision it names:
// mesh-api-00021-j42 is 21. Cloud Run numbers a service's revisions in the
// order they are created, so a later deploy has a larger one. ok is false
// for a name not of that shape (a custom --revision-suffix, a local run).
func RevisionSequence(revision string) (n int64, ok bool) {
	parts := strings.Split(revision, "-")
	if len(parts) < 3 || parts[len(parts)-1] == "" {
		return 0, false
	}
	seq := parts[len(parts)-2]
	for _, r := range seq {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(seq, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// leaseName is the application_name of a leader's lease session. Postgres
// keeps 63 bytes of it; the generation comes first so a long revision
// loses only its tail.
func leaseName(g generation, yielding bool) string {
	s := "loom lease " + strconv.FormatInt(g.n, 10)
	if yielding {
		s += " yielding"
	}
	if g.revision != "" {
		s += " " + g.revision
	}
	return s
}

// parseLeaseName reads a lease session's application_name. ok is false for
// one that is not loom's — an older loom's leader, or one with no
// generation, which is never preempted.
func parseLeaseName(app string) (g generation, yielding, ok bool) {
	f := strings.Fields(app)
	if len(f) < 3 || f[0] != "loom" || f[1] != "lease" {
		return generation{}, false, false
	}
	n, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil || n <= 0 {
		return generation{}, false, false
	}
	rest := f[3:]
	if len(rest) > 0 && rest[0] == "yielding" {
		yielding, rest = true, rest[1:]
	}
	if len(rest) > 0 {
		g.revision = rest[0]
	}
	g.n = n
	return g, yielding, true
}

// runElection keeps trying for the lease; while held, keeps the
// connection alive and watches for its loss — or, with a generation, for
// a newer generation asking for it.
func (c *Client) runElection(ctx context.Context, poll time.Duration) {
	var to takeover
	for ctx.Err() == nil {
		held, yielded, err := c.lead(ctx, poll)
		if err != nil && ctx.Err() == nil {
			c.log.WarnContext(ctx, "process election", "error", err)
		}
		if held {
			c.log.InfoContext(ctx, "process lease released")
		}
		wait := poll
		switch {
		case yielded:
			// the newer follower is trying every fastPoll: stay out of
			// its way. Should it be gone by then, this instance leads
			// again until a newer one returns.
			wait = 2 * poll
		case !held && c.gen.n > 0:
			fast, err := c.takeOver(ctx, &to, poll)
			if err != nil && ctx.Err() == nil {
				c.log.WarnContext(ctx, "process lease take-over", "error", err)
			}
			if fast {
				wait = fastPoll(poll)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// fastPoll is how often a follower taking the lease over tries for it.
func fastPoll(poll time.Duration) time.Duration {
	return min(max(poll/20, 10*time.Millisecond), 250*time.Millisecond)
}

// lead tries the lease once. A follower returns at once, holding nothing.
// A leader keeps the connection until ctx ends, the connection fails, or
// it yields to a newer generation, and reports held so the caller can log
// the hand-back, and yielded when it gave the lease to a newer one.
func (c *Client) lead(ctx context.Context, poll time.Duration) (held, yielded bool, err error) {
	conn, err := c.db.Acquire(ctx)
	if err != nil {
		return false, false, err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('loom_processes_' || $1))`, c.reg.Service).Scan(&got); err != nil {
		return false, false, err
	}
	if !got {
		return false, false, nil
	}
	if c.gen.n > 0 {
		// name the session and listen for a newer generation before the
		// runners start; on the way out the connection goes back to the
		// pool as it came, or not at all
		defer func() {
			reset, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := conn.Exec(reset, `UNLISTEN *; RESET application_name`); err != nil {
				_ = conn.Conn().Close(reset)
			}
		}()
		_, err := conn.Exec(ctx, `SELECT set_config('application_name', $1, false)`, leaseName(c.gen, false))
		if err == nil {
			_, err = conn.Exec(ctx, "LISTEN "+pgIdent(leaseChannel))
		}
		if err != nil {
			// the session holds the lock: it must not go back to the pool
			_ = conn.Conn().Close(context.Background())
			return false, false, err
		}
	}
	c.leader.Store(true)
	defer c.leader.Store(false)
	c.log.InfoContext(ctx, "process lease acquired", c.genAttrs()...)
	// the runners may be asleep on their tick: the lease is new work
	c.fan.wakeAll()

	unlock := func() {
		// hand the lease back deliberately rather than waiting for the
		// session to end; best-effort, the pool will close the
		// connection either way
		release, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = conn.Exec(release, `SELECT pg_advisory_unlock(hashtext('loom_processes_' || $1))`, c.reg.Service)
		cancel()
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		if c.gen.n > 0 {
			wait, cancel := context.WithTimeout(ctx, poll)
			n, err := conn.Conn().WaitForNotification(wait)
			cancel()
			switch {
			case ctx.Err() != nil:
				unlock()
				return true, false, nil
			case err == nil:
				if newer, ok := c.newerAsking(n.Payload); ok {
					c.yield(ctx, conn.Conn(), newer)
					unlock()
					return true, true, nil
				}
				continue
			case !errors.Is(err, context.DeadlineExceeded):
				// the session ended under it: terminated, or the
				// connection failed — the lease went with it
				return true, false, err
			}
		} else {
			select {
			case <-ctx.Done():
				unlock()
				return true, false, nil
			case <-t.C:
			}
		}
		// the lease lives with the session: a connection that no longer
		// answers has lost it, whatever this instance believes
		if _, err := conn.Exec(ctx, `SELECT 1`); err != nil {
			return true, false, err
		}
	}
}

// newerAsking reads a lease-channel payload: a follower of this service
// whose generation is strictly newer than this instance's.
func (c *Client) newerAsking(payload string) (int64, bool) {
	svc, n, ok := strings.Cut(payload, " ")
	if !ok || svc != c.reg.Service {
		return 0, false
	}
	gen, err := strconv.ParseInt(n, 10, 64)
	if err != nil || gen <= c.gen.n {
		return 0, false
	}
	return gen, true
}

// yield stops this instance's process runners taking another event and
// waits for the reaction in flight (each checkpoints per event) before
// the caller releases the lease: the newer generation starts where this
// one stopped, without the two reacting at once.
func (c *Client) yield(ctx context.Context, conn *pgx.Conn, newer int64) {
	c.log.InfoContext(ctx, "process lease yielding to a newer generation", append(c.genAttrs(), "newer", newer)...)
	// tell the follower it is heard, so it waits for the drain rather
	// than ending this session
	_, _ = conn.Exec(ctx, `SELECT set_config('application_name', $1, false)`, leaseName(c.gen, true))
	c.leader.Store(false)
	deadline := time.Now().Add(handoverDrain)
	for c.procSteps.Load() > 0 && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if n := c.procSteps.Load(); n > 0 {
		c.log.WarnContext(ctx, "process lease yielded with reactions in flight", "steps", n, "drain", handoverDrain.String())
	}
}

func (c *Client) genAttrs() []any {
	if c.gen.n <= 0 {
		return nil
	}
	return []any{"generation", c.gen.n, "revision", c.gen.revision}
}

// leaseHolder finds the session holding service's lease: loom takes it
// with pg_try_advisory_lock(hashtext(...)), the int4 widened to a bigint
// key, which pg_locks splits into classid (high half) and objid (low).
const leaseHolder = `
	SELECT a.pid, coalesce(a.application_name, '')
	FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
	WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
	  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
	  AND l.classid = ((hashtext('loom_processes_' || $1)::bigint >> 32) & 4294967295)::oid
	  AND l.objid = (hashtext('loom_processes_' || $1)::bigint & 4294967295)::oid`

// takeover is a follower's progress taking the lease from one older
// leader's session.
type takeover struct {
	pid      int32
	since    time.Time // first seen holding it
	notified time.Time // last asked to yield
}

// takeOver looks at who leads; when it is an older generation, asks it to
// yield, and ends its session when it does not answer in time. fast is
// true while the lease is about to come free, so the caller tries for it
// again within fastPoll rather than a whole poll.
func (c *Client) takeOver(ctx context.Context, to *takeover, poll time.Duration) (fast bool, err error) {
	var pid int32
	var app string
	if err := c.db.QueryRow(ctx, leaseHolder, c.reg.Service).Scan(&pid, &app); err != nil {
		*to = takeover{}
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil // free: the next try takes it
		}
		return false, err
	}
	theirs, yielding, ok := parseLeaseName(app)
	if !ok || theirs.n >= c.gen.n {
		*to = takeover{} // an equal or newer generation, or one with none
		return false, nil
	}
	now := time.Now()
	if to.pid != pid {
		*to = takeover{pid: pid, since: now}
		c.log.InfoContext(ctx, "process lease held by an older generation: asking it to yield",
			append(c.genAttrs(), "from_generation", theirs.n, "from_revision", theirs.revision)...)
	}
	if now.Sub(to.notified) >= poll {
		if _, err := c.db.Exec(ctx, `SELECT pg_notify($1, $2)`, leaseChannel,
			c.reg.Service+" "+strconv.FormatInt(c.gen.n, 10)); err != nil {
			return true, err
		}
		to.notified = now
	}
	patience := 2 * poll
	if yielding {
		patience += handoverDrain
	}
	if now.Sub(to.since) < patience {
		return true, nil
	}
	var ended bool
	if err := c.db.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&ended); err != nil {
		return true, err
	}
	c.log.WarnContext(ctx, "process lease taken from an older generation that did not yield",
		append(c.genAttrs(), "from_generation", theirs.n, "from_revision", theirs.revision,
			"yielding", yielding, "ended", ended, "waited", now.Sub(to.since).String())...)
	*to = takeover{}
	return true, nil
}

// leaderFlag is the process lease as this instance sees it.
type leaderFlag struct{ atomic.Bool }
