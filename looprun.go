package loom

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The loop runner. A @loop aggregate's look is called for each live
// instance by the rule in loop.go: no later than LoopEvery after its
// last look, within LoopSoon of an event for it, and at the first
// deadline among its waits. What the runtime has noted about each
// instance is one row of loom_loop_due:
//
//   - the first event of an instance writes its row, due at once;
//   - every later event sets event_at and brings next_look to within
//     LoopSoon, on the transaction that appends the event, so a rolled
//     back unit of work moves nothing;
//   - the runner claims the rows that are due, calls each look, runs
//     the commands it answers with as one unit of work, and writes the
//     next look by LoopState.NextLook.
//
// A look that says LookDone ends the instance's looks: its row is
// dropped, and an event that arrives for it later starts them again.
//
// Looks run on every instance of the newest generation alive; a row is
// claimed with a lease, so two runners never look at one instance at
// once, and one that dies mid-look gives it up when the lease runs out.

const (
	// loopPoll is how often the runner asks for due looks. It bounds how
	// late a look is past its due time.
	loopPoll = 250 * time.Millisecond
	// loopLease is how long a claimed look is another runner's to leave
	// alone.
	loopLease = 30 * time.Second
	// loopBatch is how many due looks one pass claims.
	loopBatch = 50
)

// loops reports whether any aggregate of the registry declares a loop.
func (c *Client) loops() bool {
	for _, a := range c.reg.Aggregates {
		if a.Look != nil {
			return true
		}
	}
	return false
}

func (c *Client) looping(aggregate string) *AggregateDef {
	for _, a := range c.reg.Aggregates {
		if a.Name == aggregate && a.Look != nil {
			return a
		}
	}
	return nil
}

// noteLoopEvents brings forward the next look of each looping instance
// evts belong to: created due at once, and after that within LoopSoon
// of the event.
func (c *Client) noteLoopEvents(ctx context.Context, tx pgx.Tx, evts []*Event) error {
	type instance struct {
		aggregate, namespace string
		id                   uuid.UUID
	}
	seen := map[instance]bool{}
	for _, e := range evts {
		in := instance{e.AggregateType, e.Namespace, e.AggregateID}
		if seen[in] || c.looping(e.AggregateType) == nil {
			continue
		}
		seen[in] = true
		_, err := tx.Exec(ctx, `
			INSERT INTO loom_loop_due (service, aggregate_type, namespace, aggregate_id, next_look, event_at)
			VALUES ($1,$2,$3,$4, clock_timestamp(), clock_timestamp())
			ON CONFLICT (service, aggregate_type, namespace, aggregate_id) DO UPDATE
			SET event_at = clock_timestamp(),
			    next_look = LEAST(loom_loop_due.next_look, clock_timestamp() + $5 * interval '1 microsecond')`,
			c.reg.Service, e.AggregateType, e.Namespace, e.AggregateID, LoopSoon.Microseconds())
		if err != nil {
			return err
		}
	}
	return nil
}

// startLoopRunner looks at due instances until ctx ends, once the
// instance knows where it stands among the generations.
func (c *Client) startLoopRunner(ctx context.Context) {
	if !c.loops() {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-c.beaten:
		}
		t := time.NewTicker(loopPoll)
		defer t.Stop()
		for {
			// an instance a newer generation has superseded leaves the
			// looks to it: they are the newest build's code to run
			for c.newest() {
				n, err := c.lookDue(ctx, loopBatch)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.log.ErrorContext(ctx, "loop runner failed", "error", err)
					break
				}
				if n < loopBatch {
					break
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// dueLook is one claimed row.
type dueLook struct {
	aggregate, namespace string
	id                   uuid.UUID
	state                LoopState
}

// lookDue claims up to limit due looks and makes each. It answers how
// many it claimed.
func (c *Client) lookDue(ctx context.Context, limit int) (int, error) {
	rows, err := c.db.Query(ctx, `
		UPDATE loom_loop_due d SET leased_until = now() + $3 * interval '1 second'
		FROM (
			SELECT aggregate_type, namespace, aggregate_id FROM loom_loop_due
			WHERE service=$1 AND next_look <= now() AND (leased_until IS NULL OR leased_until < now())
			ORDER BY next_look
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		) due
		WHERE d.service=$1 AND d.aggregate_type=due.aggregate_type AND d.namespace=due.namespace AND d.aggregate_id=due.aggregate_id
		RETURNING d.aggregate_type, d.namespace, d.aggregate_id, d.looked_at, d.event_at, d.waits`,
		c.reg.Service, limit, int(loopLease.Seconds()))
	if err != nil {
		return 0, err
	}
	var due []dueLook
	for rows.Next() {
		var (
			l             dueLook
			looked, event *time.Time
			waits         []byte
		)
		if err := rows.Scan(&l.aggregate, &l.namespace, &l.id, &looked, &event, &waits); err != nil {
			rows.Close()
			return 0, err
		}
		if looked != nil {
			l.state.Looked = *looked
		}
		if event != nil {
			l.state.Event = *event
		}
		_ = json.Unmarshal(waits, &l.state.Waits)
		due = append(due, l)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, l := range due {
		if err := c.look(ctx, l); err != nil {
			if ctx.Err() != nil {
				return len(due), err
			}
			c.log.ErrorContext(ctx, "look failed: it is looked at again in a minute", "aggregate", l.aggregate, "namespace", l.namespace, "id", l.id, "error", err)
		}
	}
	return len(due), nil
}

// look makes one claimed look and writes when the next is. A look that
// fails, or whose commands are refused, is noted as made: the instance
// is looked at again by the rule, a minute on at the latest, and what
// it decides then it decides afresh.
func (c *Client) look(ctx context.Context, l dueLook) error {
	def := c.looping(l.aggregate)
	// the look is of the instant it starts: an event that lands while
	// it runs, its own commands' included, has arrived since
	var start time.Time
	if err := c.db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&start); err != nil {
		return err
	}
	if def == nil {
		// this build's registry has no loop for it: leave the row to a
		// build that does
		return c.looked(ctx, l, start, nil, 0)
	}
	told := l.state.Look(start)
	told.Aggregate, told.Namespace, told.ID = l.aggregate, l.namespace, l.id
	said := &lookSaid{}
	lctx := withReader(ctx, c)
	lctx = WithLooking(lctx, told)
	lctx = context.WithValue(lctx, lookSaidKey{}, said)
	cmds, lookErr := def.Look(lctx)
	if lookErr == nil && len(cmds) > 0 {
		lookErr = c.Dispatch(WithMeta(ctx, Metadata{CausationID: "loop:" + l.aggregate + "/" + l.id.String()}), cmds...)
	}
	if lookErr == nil && said.done {
		// done, unless an event has arrived since the look began
		_, err := c.db.Exec(ctx, `
			DELETE FROM loom_loop_due
			WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4
			  AND (event_at IS NULL OR event_at <= $5)`,
			c.reg.Service, l.aggregate, l.namespace, l.id, start)
		if err != nil {
			return err
		}
	}
	// the waits the look was told have expired are spent
	if err := c.looked(ctx, l, start, told.Waiting, said.again); err != nil {
		return err
	}
	return lookErr
}

// looked notes a look made at start: the lease given up, the waits left,
// and the next look by LoopState.NextLook over what the row now holds —
// an event that arrived after start brings it to within LoopSoon.
func (c *Client) looked(ctx context.Context, l dueLook, start time.Time, waiting []Wait, again time.Duration) error {
	if waiting == nil {
		waiting = []Wait{}
	}
	raw, err := json.Marshal(waiting)
	if err != nil {
		return err
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var event *time.Time
	err = tx.QueryRow(ctx, `
		SELECT event_at FROM loom_loop_due
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4
		FOR UPDATE`,
		c.reg.Service, l.aggregate, l.namespace, l.id).Scan(&event)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // done with, by LookDone
	}
	if err != nil {
		return err
	}
	state := LoopState{Looked: start, Waits: waiting}
	if event != nil {
		state.Event = *event
	}
	next := state.NextLook()
	if again > 0 {
		if asked := start.Add(again); asked.Before(next) {
			next = asked
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE loom_loop_due SET looked_at=$5, next_look=$6, waits=$7, leased_until=NULL
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
		c.reg.Service, l.aggregate, l.namespace, l.id, start, next, raw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// LoopDue is what the runtime holds about one looping instance.
type LoopDue struct {
	Aggregate string    `json:"aggregate"`
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
	// NextLook is when it is next looked at.
	NextLook time.Time `json:"next_look"`
	LoopState
}

// LoopDue reads when an instance of a @loop aggregate is next looked
// at, and what has been noted about it since its last look. ok is false
// for an instance with no looks due: never created, or done.
func (c *Client) LoopDue(ctx context.Context, aggregate, namespace string, id uuid.UUID) (due LoopDue, ok bool, err error) {
	var (
		looked, event *time.Time
		waits         []byte
	)
	err = c.db.QueryRow(ctx, `
		SELECT next_look, looked_at, event_at, waits FROM loom_loop_due
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
		c.reg.Service, aggregate, namespace, id).Scan(&due.NextLook, &looked, &event, &waits)
	if errors.Is(err, pgx.ErrNoRows) {
		return due, false, nil
	}
	if err != nil {
		return due, false, err
	}
	due.Aggregate, due.Namespace, due.ID = aggregate, namespace, id
	if looked != nil {
		due.Looked = *looked
	}
	if event != nil {
		due.Event = *event
	}
	_ = json.Unmarshal(waits, &due.Waits)
	return due, true, nil
}
