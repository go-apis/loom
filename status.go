package loom

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Status is what a service's background work is doing right now: which
// instance leads its processes, how far each projection and process is
// behind the log, and what is waiting — timers, effects, the outbox,
// dead letters, held plans. It is read from the database, so any
// instance answers for the whole service, not for itself.
type Status struct {
	Service string `json:"service"`
	// Latest is the newest event's position in the log.
	Latest int64 `json:"latest"`
	// Lease is the instance running the service's processes.
	Lease LeaseStatus `json:"lease"`
	// Instances is the builds with a live instance, newest first: after
	// a deploy, the old revision's line is how long it lingers. Only
	// the newest fires timers. Empty for a service with no generation.
	Instances []InstanceStatus `json:"instances"`
	// HereNewest is whether the instance that answered is of the newest
	// generation alive, as it last saw: false on an old revision's
	// instance once it has learned of the deploy, and from then it
	// leaves timers, batches and projections to the newer build.
	HereNewest bool `json:"here_newest"`
	// Runners is every projection and process with its position.
	Runners []RunnerStatus `json:"runners"`
	// Behind counts the runners with events still to handle, and Stalled
	// those halted on one they cannot.
	Behind  int `json:"behind"`
	Stalled int `json:"stalled"`

	Timers TimerStatus `json:"timers"`
	// Schedules is the commands dispatched on an interval, and when
	// each is next due.
	Schedules []ScheduleStatus `json:"schedules"`
	// EffectsRunning and EffectsFailed count journaled outside calls in
	// flight and given up on.
	EffectsRunning int64 `json:"effects_running"`
	EffectsFailed  int64 `json:"effects_failed"`
	// OutboxDepth is events not yet published to the bus, and
	// OutboxOldest when the oldest of them was written.
	OutboxDepth  int64      `json:"outbox_depth"`
	OutboxOldest *time.Time `json:"outbox_oldest,omitempty"`
	// DeadLetters counts what was parked for a person to look at.
	DeadLetters int64 `json:"dead_letters"`
	// Plans counts commands held for RunPlan and not yet expired.
	Plans int64 `json:"plans"`
}

// LeaseStatus is who holds the process lease (election.go).
type LeaseStatus struct {
	// Needed is whether the service has processes that react to its own
	// events: only those run under the lease. A service with none
	// elects nobody, and Held is false for it by design.
	Needed bool `json:"needed"`
	// Held is false when no instance leads: where a lease is Needed,
	// nothing is reacting to local events until one takes it.
	Held bool `json:"held"`
	// Generation and Revision are the leader's build, as its lease
	// session names it; zero and empty for a leader with no generation.
	Generation int64  `json:"generation,omitempty"`
	Revision   string `json:"revision,omitempty"`
	// Yielding marks a leader handing over to a newer generation.
	Yielding bool `json:"yielding,omitempty"`
	// Here is whether the instance that answered is the leader, and
	// HereGeneration and HereRevision that instance's own build: a
	// leader on an older build than the one answering is a deploy whose
	// hand-over has not happened.
	Here           bool   `json:"here"`
	HereGeneration int64  `json:"here_generation,omitempty"`
	HereRevision   string `json:"here_revision,omitempty"`
}

// TimerStatus is the pending schedule.
type TimerStatus struct {
	Pending int64 `json:"pending"`
	// Overdue is those whose time has passed and have not fired, and
	// OldestDue the earliest fire time of any.
	Overdue   int64      `json:"overdue"`
	OldestDue *time.Time `json:"oldest_due,omitempty"`
}

// RunnerStatus is one projection or process against the log.
type RunnerStatus struct {
	Runner string `json:"runner"`
	// Kind: projection | process | process(bus) — bus consumers have no
	// checkpoint, they dedup instead.
	Kind      string     `json:"kind"`
	Seq       int64      `json:"seq"`
	Latest    int64      `json:"latest"`
	Lag       int64      `json:"lag"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	// BehindSince is when the oldest event this runner has not handled
	// was written: how long it has been behind, where Lag is how far.
	// Nil for a runner that is caught up.
	BehindSince *time.Time `json:"behind_since,omitempty"`
	// Stalled: a projection halted on an event it cannot fold or write —
	// FailingSeq is that event, LastError why, StalledSince the first
	// failure. POST /runners/{name}/skip parks it and moves on.
	Stalled      bool       `json:"stalled"`
	FailingSeq   int64      `json:"failing_seq,omitempty"`
	Attempts     int        `json:"attempts,omitempty"`
	LastError    string     `json:"last_error,omitempty"`
	StalledSince *time.Time `json:"stalled_since,omitempty"`
}

// Status reads the service's background work.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	latest, runners, err := c.runners(ctx)
	if err != nil {
		return nil, err
	}
	st := &Status{Service: c.reg.Service, Latest: latest, Runners: runners, HereNewest: !c.superseded.Load()}
	if st.Instances, err = c.instances(ctx); err != nil {
		return nil, err
	}
	if st.Schedules, err = c.schedules(ctx); err != nil {
		return nil, err
	}
	for _, r := range runners {
		if r.Lag > 0 {
			st.Behind++
		}
		if r.Stalled {
			st.Stalled++
		}
	}

	st.Lease = LeaseStatus{Here: c.Leading(), HereGeneration: c.gen.n, HereRevision: c.gen.revision}
	for _, p := range c.reg.Processes {
		if local, _ := c.splitSubscriptions(p); len(local) > 0 {
			st.Lease.Needed = true
		}
	}
	var pid int32
	var app string
	switch err := c.db.QueryRow(ctx, leaseHolder, c.reg.Service).Scan(&pid, &app); {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return nil, err
	default:
		st.Lease.Held = true
		if g, yielding, ok := parseLeaseName(app); ok {
			st.Lease.Generation, st.Lease.Revision, st.Lease.Yielding = g.n, g.revision, yielding
		}
	}

	if err := c.db.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE fire_at <= now()), min(fire_at)
		FROM loom_timers WHERE service=$1`, c.reg.Service).
		Scan(&st.Timers.Pending, &st.Timers.Overdue, &st.Timers.OldestDue); err != nil {
		return nil, err
	}
	if err := c.db.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status='running'), count(*) FILTER (WHERE status='failed')
		FROM loom_effects WHERE service=$1`, c.reg.Service).Scan(&st.EffectsRunning, &st.EffectsFailed); err != nil {
		return nil, err
	}
	if err := c.db.QueryRow(ctx, `
		SELECT count(*), min(created_at) FROM loom_outbox
		WHERE service=$1 AND published_at IS NULL`, c.reg.Service).Scan(&st.OutboxDepth, &st.OutboxOldest); err != nil {
		return nil, err
	}
	if err := c.db.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters WHERE service=$1`, c.reg.Service).Scan(&st.DeadLetters); err != nil {
		return nil, err
	}
	if err := c.db.QueryRow(ctx, `SELECT count(*) FROM loom_plans WHERE service=$1 AND expires_at > now()`, c.reg.Service).Scan(&st.Plans); err != nil {
		return nil, err
	}
	return st, nil
}

// runners is every projection and process against the log's head.
func (c *Client) runners(ctx context.Context) (latest int64, out []RunnerStatus, err error) {
	if err := c.db.QueryRow(ctx, `SELECT coalesce(max(global_seq),0) FROM loom_events WHERE service=$1`, c.reg.Service).Scan(&latest); err != nil {
		return 0, nil, err
	}
	type cp struct {
		seq        int64
		at         time.Time
		failingSeq int64
		attempts   int
		lastError  string
		since      *time.Time
	}
	cps := map[string]cp{}
	rows, err := c.db.Query(ctx, `
		SELECT runner, global_seq, updated_at, failing_seq, attempts, last_error, stalled_since
		FROM loom_checkpoints WHERE service=$1`, c.reg.Service)
	if err != nil {
		return 0, nil, err
	}
	for rows.Next() {
		var name string
		var v cp
		if err := rows.Scan(&name, &v.seq, &v.at, &v.failingSeq, &v.attempts, &v.lastError, &v.since); err != nil {
			rows.Close()
			return 0, nil, err
		}
		cps[name] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	out = []RunnerStatus{}
	// runners behind at the same position share one look-up of when the
	// next event was written
	since := map[int64]*time.Time{}
	behindSince := func(seq int64) (*time.Time, error) {
		if at, ok := since[seq]; ok {
			return at, nil
		}
		var at time.Time
		err := c.db.QueryRow(ctx, `
			SELECT at FROM loom_events WHERE service=$1 AND global_seq > $2
			ORDER BY global_seq LIMIT 1`, c.reg.Service, seq).Scan(&at)
		if errors.Is(err, pgx.ErrNoRows) {
			since[seq] = nil
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		since[seq] = &at
		return &at, nil
	}
	add := func(runner, kind string, checkpointed bool) error {
		st := RunnerStatus{Runner: runner, Kind: kind, Latest: latest}
		if v, ok := cps[runner]; ok {
			at := v.at
			st.Seq, st.UpdatedAt = v.seq, &at
			st.Stalled = v.since != nil
			st.FailingSeq, st.Attempts, st.LastError, st.StalledSince = v.failingSeq, v.attempts, v.lastError, v.since
		}
		if checkpointed {
			st.Lag = latest - st.Seq
			if st.Lag > 0 {
				at, err := behindSince(st.Seq)
				if err != nil {
					return err
				}
				st.BehindSince = at
			}
		}
		out = append(out, st)
		return nil
	}
	for _, p := range c.reg.Projections {
		if err := add("projection:"+p.Name, "projection", true); err != nil {
			return 0, nil, err
		}
	}
	for _, p := range c.reg.Processes {
		local, foreign := c.splitSubscriptions(p)
		if len(local) > 0 {
			if err := add("process:"+p.Name, "process", true); err != nil {
				return 0, nil, err
			}
		}
		if len(foreign) > 0 {
			if err := add("process:"+p.Name+" (bus)", "process(bus)", false); err != nil {
				return 0, nil, err
			}
		}
	}
	return latest, out, nil
}
