package loom

import "context"

// A process reads the whole log but reacts to a few types of event. It
// used to write its checkpoint after every event it read, its own or
// not: 84,000 events of one type cost every other process 84,000 writes
// apiece, a few a second through the shared step slots, for hours
// (runsheet, 2026-11-18). Now a run of events the process does not
// subscribe to is passed in one write: to the last ignored event before
// the next subscribed one, or to the end of the run.
//
// What may be written is unchanged in kind: the checkpoint only ever
// holds a sequence S such that every subscribed event at or before S was
// handled, parked or handed to a durable retry by this process. A run is
// passed by a write that moves the checkpoint only from where this step
// left it (passCheckpoint), so a step that has lost its place — a reset,
// another leader — passes nothing and reads again.

// ignoredScan bounds how far past a batch one statement looks for the
// end of a run of ignored events: the run costs one checkpoint write per
// this many events, not one per event.
const ignoredScan = 50 * logBatch

// ignoredRun counts the events at the head of events that the runner
// does not subscribe to.
func ignoredRun(events []*Event, subscribed []string) int {
	for i, evt := range events {
		if contains(subscribed, evt.Type) {
			return i
		}
	}
	return len(events)
}

// processWalk is one process step's walk over a batch, apart from the
// store and the reaction, so it can be driven without either.
type processWalk struct {
	subscribed []string
	// leading reports whether this instance still holds the process
	// lease; the walk stops between events once it does not.
	leading func() bool
	// handle delivers one subscribed event: nil once the process may
	// move past it (reacted, parked, or owned by a durable retry).
	handle func(ctx context.Context, evt *Event) error
	// save writes the checkpoint after a handled event.
	save func(ctx context.Context, seq int64) error
	// pass moves the checkpoint over ignored events, from where the walk
	// left it; false when the checkpoint was no longer there.
	pass func(ctx context.Context, from, to int64) (bool, error)
	// ahead answers the last event of the ignored run that carries on
	// past after (after itself when the next event is subscribed, or
	// there is none).
	ahead func(ctx context.Context, after int64) (int64, error)
}

// walk takes the batch read after the checkpoint at seq. full says the
// read came back at its limit, so the log may go on past it. It answers
// how many events it got through, as the step does: zero is an idle
// step.
func (w processWalk) walk(ctx context.Context, seq int64, events []*Event, full bool) (int, error) {
	for i := 0; i < len(events); {
		if i > 0 && !w.leading() {
			// the lease went (yielded to a newer generation, or lost):
			// stop at the last checkpointed event; the next leader
			// starts after it
			return i, nil
		}
		if run := ignoredRun(events[i:], w.subscribed); run > 0 {
			to := events[i+run-1].GlobalSeq
			if full && i+run == len(events) {
				// the batch ended inside the run: find where the run
				// ends rather than reading it 200 events at a time
				var err error
				if to, err = w.ahead(ctx, to); err != nil {
					return 0, err
				}
			}
			moved, err := w.pass(ctx, seq, to)
			if err != nil {
				return 0, err
			}
			if !moved {
				// the checkpoint is not where this step left it: read
				// again from wherever it is now
				return i + run, nil
			}
			seq = to
			i += run
			continue
		}
		evt := events[i]
		if err := w.handle(ctx, evt); err != nil {
			return 0, err
		}
		if err := w.save(ctx, evt.GlobalSeq); err != nil {
			return 0, err
		}
		seq = evt.GlobalSeq
		i++
	}
	return len(events), nil
}

// ignoredAhead answers the last event of the run of events this service
// logged after `after` whose types are not in subscribed, looking at
// most ignoredScan events on: the event just before the next subscribed
// one, the last of the events looked at when none is subscribed, or
// after itself when the very next event is subscribed or there is none.
//
// One statement, so one snapshot. Appends to a service's log commit in
// sequence order under its log lock (db.go lockLog), so an event the
// snapshot does not see is later than every one it does, exactly as for
// readLog: nothing subscribed can turn up behind the answer.
func (c *Client) ignoredAhead(ctx context.Context, q querier, after int64, subscribed []string) (int64, error) {
	if subscribed == nil {
		subscribed = []string{}
	}
	var to int64
	err := q.QueryRow(ctx, `
		WITH w AS (
			SELECT global_seq, type FROM loom_events
			WHERE service=$1 AND global_seq > $2
			ORDER BY global_seq ASC
			LIMIT $4
		), n AS (
			SELECT min(global_seq) AS seq FROM w WHERE type = ANY($3::text[])
		)
		SELECT coalesce(max(w.global_seq), $2::bigint)
		FROM w, n
		WHERE n.seq IS NULL OR w.global_seq < n.seq`,
		c.reg.Service, after, subscribed, ignoredScan).Scan(&to)
	return to, err
}

// passCheckpoint moves a runner's checkpoint from one sequence to a
// later one over events it does not subscribe to, and reports whether it
// did. It writes only while the checkpoint is still at from (or, from
// zero, has no row yet): passing handles nothing, so it has no claim on a checkpoint
// somebody else has moved.
func passCheckpoint(ctx context.Context, q executor, service, runner string, from, to int64) (bool, error) {
	const cleared = `global_seq = $3, updated_at = now(),
			failing_seq = 0, attempts = 0, last_error = '', stalled_since = NULL`
	sql := `
		UPDATE loom_checkpoints SET ` + cleared + `
		WHERE service=$1 AND runner=$2 AND global_seq = $4`
	if from == 0 {
		// a runner that has not checkpointed yet has no row to move
		sql = `
		INSERT INTO loom_checkpoints AS c (service, runner, global_seq, updated_at)
		VALUES ($1,$2,$3, now())
		ON CONFLICT (service, runner) DO UPDATE SET ` + cleared + `
		WHERE c.global_seq = $4`
	}
	tag, err := q.Exec(ctx, sql, service, runner, to, from)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
