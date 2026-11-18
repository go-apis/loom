package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
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
// A look that fails — it errs, a command it answers with is refused, or
// it panics, which is recovered and never kills the process — is
// counted (failures, last_error): the next is LoopBackoff away, and
// once Config.LoopFailureLimit fail in a row the instance is parked
// with a wait on its operators (LoopState.ParkPast). A look that
// succeeds clears the count and unparks it.
//
// A look that keeps sending the same commands while nothing recorded
// about its instance changes is held back (LoopState.Acting, loop.go): the events
// of a look's own commands are noted apart from events from outside
// (outside_at), the run is counted on the row (acting), and the look
// that begins a spell tells the service (Config.OnLoopActing).
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

// loopInstance names one instance of a @loop aggregate.
type loopInstance struct {
	aggregate, namespace string
	id                   uuid.UUID
}

type ownLookKey struct{}

// withOwnLook marks ctx as the unit of work of a look's own commands:
// the events it appends on that instance are the look's doing.
func withOwnLook(ctx context.Context, in loopInstance) context.Context {
	return context.WithValue(ctx, ownLookKey{}, in)
}

// noteLoopEvents brings forward the next look of each looping instance
// evts belong to: created due at once, and after that within LoopSoon
// of the event. An event the instance's own look caused — appended by
// the unit of work of the commands it answered with — is noted as an
// event (event_at) and not as one from outside (outside_at): the look
// that ends writes the next look itself, and holds it back when its
// looks keep acting without progress (LoopState.NextLook).
func (c *Client) noteLoopEvents(ctx context.Context, tx pgx.Tx, evts []*Event) error {
	own, _ := ctx.Value(ownLookKey{}).(loopInstance)
	seen := map[loopInstance]bool{}
	for _, e := range evts {
		in := loopInstance{e.AggregateType, e.Namespace, e.AggregateID}
		if seen[in] || c.looping(e.AggregateType) == nil {
			continue
		}
		seen[in] = true
		outside := in != own
		_, err := tx.Exec(ctx, `
			INSERT INTO loom_loop_due (service, aggregate_type, namespace, aggregate_id, next_look, event_at, outside_at)
			VALUES ($1,$2,$3,$4, clock_timestamp(), clock_timestamp(), CASE WHEN $6 THEN clock_timestamp() END)
			ON CONFLICT (service, aggregate_type, namespace, aggregate_id) DO UPDATE
			SET event_at = clock_timestamp(),
			    outside_at = CASE WHEN $6 THEN clock_timestamp() ELSE loom_loop_due.outside_at END,
			    next_look = LEAST(loom_loop_due.next_look, clock_timestamp() + $5 * interval '1 microsecond')`,
			c.reg.Service, e.AggregateType, e.Namespace, e.AggregateID, LoopSoon.Microseconds(), outside)
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
	// fired is the declared deadlines that expired and have held since
	fired []string
	// notes is what happened to the instance's children since its last
	// look (supervise.go)
	notes []ChildNote
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
		RETURNING d.aggregate_type, d.namespace, d.aggregate_id, d.looked_at, d.event_at, d.waits, d.deadlines_fired, d.child_notes`,
		c.reg.Service, limit, int(loopLease.Seconds()))
	if err != nil {
		return 0, err
	}
	var due []dueLook
	for rows.Next() {
		var (
			l             dueLook
			looked, event *time.Time
			waits, fired  []byte
			notes         []byte
		)
		if err := rows.Scan(&l.aggregate, &l.namespace, &l.id, &looked, &event, &waits, &fired, &notes); err != nil {
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
		_ = json.Unmarshal(fired, &l.fired)
		_ = json.Unmarshal(notes, &l.notes)
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
			c.log.ErrorContext(ctx, "look failed: it is looked at again after a backoff, and parked for its operators past the failure limit", "aggregate", l.aggregate, "namespace", l.namespace, "id", l.id, "error", err)
		}
	}
	return len(due), nil
}

// look makes one claimed look and writes when the next is. A look that
// fails, panics, or whose commands are refused, is noted as made and
// counted: the instance is looked at again after LoopBackoff of its
// failures, or parked past the limit, and what it decides then it
// decides afresh.
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
		_, err := c.looked(ctx, l, start, Looking{}, nil, l.fired, false, nil, nil, 0)
		return err
	}
	var (
		parent *ChildRef // the instance's supervisor, when it declares one
		agg    AggregateState
	)
	if len(def.Deadlines) > 0 || def.Parent != nil {
		// the declared deadlines' waits follow the state as it stands,
		// and the state names the instance's parent
		state, _, err := c.load(ctx, c.db, l.aggregate, l.namespace, l.id)
		if err != nil {
			return err
		}
		if def.Parent != nil {
			if pid := uuidOf(def.Parent.ID(state)); pid != uuid.Nil {
				parent = &ChildRef{Aggregate: def.Parent.Aggregate, Namespace: l.namespace, ID: pid}
			}
		}
		agg = state
	}
	turn := StartLook(l.state, l.fired, def.Deadlines, agg, start)
	// what AskAgent needs to know, beside whatever the deadlines said
	turn.Mesh(def.Agents, c.mesh != nil, c.meshTenant)
	told := &turn.Told
	told.Aggregate, told.Namespace, told.ID = l.aggregate, l.namespace, l.id
	if c.supervises(l.aggregate) {
		children, err := c.childrenOf(ctx, l.aggregate, l.namespace, l.id)
		if err != nil {
			return err
		}
		told.Children, told.ChildNotes = children, l.notes
	}
	// an approval nobody answered takes its deadline action before the
	// look that is told its wait has expired
	if err := c.settleAtDeadline(ctx, l.aggregate, l.namespace, l.id, start, told.Expired); err != nil {
		return err
	}
	var err error
	if told.Approvals, err = c.approvalsOf(ctx, l.aggregate, l.namespace, l.id); err != nil {
		return err
	}
	// an agent that did not answer in time is expired before the look
	// that is told its wait has
	if len(def.Agents) > 0 {
		if err := c.expireAgents(ctx, l.aggregate, l.namespace, l.id, told.Expired); err != nil {
			return err
		}
		if told.Agents, err = c.agentsOf(ctx, l.aggregate, l.namespace, l.id); err != nil {
			return err
		}
	}
	lctx := turn.Context(withReader(ctx, c))
	var sent []string // the commands the look answered with, by type
	lookErr := func() (err error) {
		// a look that panics has failed; it does not take the process
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("loom: look at %s %s panicked: %v", l.aggregate, l.id, r)
				c.log.ErrorContext(ctx, "look panicked", "aggregate", l.aggregate, "namespace", l.namespace, "id", l.id, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			}
		}()
		cmds, err := def.Look(lctx)
		if err == nil && len(cmds) > 0 {
			sent = make([]string, len(cmds))
			for i, cmd := range cmds {
				sent[i] = cmd.LoomCommand()
			}
			turn.Sent(cmds...)
			// the events these commands emit on the instance are the
			// look's own: they do not count as news from outside
			dctx := withOwnLook(ctx, loopInstance{l.aggregate, l.namespace, l.id})
			err = c.Dispatch(WithMeta(dctx, Metadata{CausationID: "loop:" + l.aggregate + "/" + l.id.String()}), cmds...)
		}
		return err
	}()
	// a look that failed started and ended nothing: what it said is
	// dropped with it, and the waits it was told had expired are told
	// again
	turn.settle(lookErr)
	if turn.Done() {
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
	if len(turn.said.agents) > 0 {
		begin, err := c.openAgents(ctx, l.aggregate, l.namespace, l.id, turn.said.agents, start)
		if err != nil {
			return err
		}
		turn.said.waits = append(turn.said.waits, begin...)
	}
	if asked := turn.Approvals(); len(asked) > 0 {
		if err := c.writeApprovals(ctx, c.db, l.aggregate, l.namespace, l.id, asked); err != nil {
			return err
		}
	}
	after, err := c.looked(ctx, l, start, turn.Told, turn.said, turn.Fired, true, lookErr, parent, len(turn.Told.ChildNotes))
	if err != nil {
		return err
	}
	parked, lastErr := after.parked, after.state.LastError
	// the look that makes it LoopActingFrom in a row begins the spell:
	// said once, here, and not again until something has moved
	if after.state.Acting == LoopActingFrom {
		c.loopActing(ctx, l, sent, after)
	}
	// the open requests with no job yet, this look's among them, are
	// offered once their waits are held
	if len(def.Agents) > 0 {
		c.offerAgents(ctx, l.aggregate, l.namespace, l.id)
	}
	// what the parent is told, outside this instance's row lock
	if parent != nil {
		self := ChildRef{Aggregate: l.aggregate, Namespace: l.namespace, ID: l.id}
		var notes []ChildNote
		if parked {
			notes = append(notes, ChildNote{ChildRef: self, What: ChildParked, LastError: lastErr, At: start})
		}
		for _, w := range turn.Told.Expired {
			notes = append(notes, ChildNote{ChildRef: self, What: ChildWaitExpired, Wait: w.Name, At: start})
		}
		if turn.Done() {
			notes = append(notes, ChildNote{ChildRef: self, What: ChildDone, At: start})
		}
		if err := c.noteParent(ctx, *parent, notes); err != nil {
			return err
		}
	}
	if restarts := turn.Restarts(); lookErr == nil && len(restarts) > 0 {
		if err := c.restartChildren(ctx, ChildRef{Aggregate: l.aggregate, Namespace: l.namespace, ID: l.id}, restarts); err != nil {
			return err
		}
	}
	return lookErr
}

// looked notes a look made at start: the lease given up, the waits as
// the look left them (settleWaits, over what the row holds now, so a
// wait started from outside while it ran is kept), the look's outcome
// when made (LoopState.AfterLook, then ParkPast the failure limit), the
// declared deadlines that have fired, and the next look by
// LoopState.NextLook — an event that arrived after start brings it to
// within LoopSoon. A look not made (no loop in this build) leaves the
// failures as they stand.
// It answers the instance as the look left it, whether this look parked
// it, and when it is next looked at. parent is recorded on the row;
// toldNotes is how many of the row's child notes the look was told,
// dropped now that it was.
func (c *Client) looked(ctx context.Context, l dueLook, start time.Time, told Looking, said *lookSaid, fired []string, made bool, lookErr error, parent *ChildRef, toldNotes int) (after lookedAs, err error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return after, err
	}
	defer tx.Rollback(ctx)
	var (
		looked, event, outside *time.Time
		held                   []byte
		notes                  []byte
		prior                  LoopState
	)
	err = tx.QueryRow(ctx, `
		SELECT looked_at, event_at, outside_at, acting, acting_sent, waits, failures, last_error, parked, child_notes FROM loom_loop_due
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4
		FOR UPDATE`,
		c.reg.Service, l.aggregate, l.namespace, l.id).Scan(&looked, &event, &outside, &prior.Acting, &prior.Sent, &held, &prior.Failures, &prior.LastError, &prior.Parked, &notes)
	if errors.Is(err, pgx.ErrNoRows) {
		return after, nil // done with, by LookDone
	}
	if err != nil {
		return after, err
	}
	var holding []Wait
	_ = json.Unmarshal(held, &holding)
	prior.Waits = holding
	if looked != nil {
		prior.Looked = *looked
	}
	if event != nil {
		prior.Event = *event
	}
	if outside != nil {
		prior.Outside = *outside
	}
	state, next := lookEnded(prior, start, told, said, made, lookErr, c.loopFails)
	raw, err := json.Marshal(state.Waits)
	if err != nil {
		return after, err
	}
	if fired == nil {
		fired = []string{}
	}
	firedRaw, err := json.Marshal(fired)
	if err != nil {
		return after, err
	}
	// the notes the look was told are done with; any that came while it
	// ran are kept for the next
	var pending []ChildNote
	_ = json.Unmarshal(notes, &pending)
	if lookErr == nil && toldNotes > 0 {
		pending = pending[min(toldNotes, len(pending)):]
	}
	if pending == nil {
		pending = []ChildNote{}
	}
	notesRaw, err := json.Marshal(pending)
	if err != nil {
		return after, err
	}
	var parentType *string
	var parentID *uuid.UUID
	if parent != nil {
		parentType, parentID = &parent.Aggregate, &parent.ID
	}
	if _, err := tx.Exec(ctx, `
		UPDATE loom_loop_due SET looked_at=$5, next_look=$6, waits=$7, leased_until=NULL,
		       failures=$8, last_error=$9, parked=$10, deadlines_fired=$11, child_notes=$12,
		       parent_type=coalesce($13, parent_type), parent_id=coalesce($14, parent_id), acting=$15, acting_sent=$16
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
		c.reg.Service, l.aggregate, l.namespace, l.id, start, next, raw, state.Failures, state.LastError, state.Parked, firedRaw, notesRaw, parentType, parentID, state.Acting, state.Sent); err != nil {
		return after, err
	}
	if err := tx.Commit(ctx); err != nil {
		return after, err
	}
	return lookedAs{state: state, next: next, parked: state.Parked && !prior.Parked}, nil
}

// lookedAs is how a look left its instance: its loop state, when it is
// next looked at, and whether this look parked it.
type lookedAs struct {
	state  LoopState
	next   time.Time
	parked bool
}

// LoopActing is what OnLoopActing is told: the looks of one instance of
// a @loop aggregate keep sending the same commands and nothing loom
// records about the instance has changed between them.
type LoopActing struct {
	// Service, Aggregate, Namespace and ID name the instance.
	Service   string    `json:"service"`
	Aggregate string    `json:"aggregate"`
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
	// Looks is how many looks in a row have acted without progress.
	Looks int `json:"looks"`
	// Commands is what the last of them sent, by type.
	Commands []string `json:"commands"`
	// At is when that look was made, and NextLook when the instance is
	// looked at next unless something from outside arrives first.
	At       time.Time `json:"at"`
	NextLook time.Time `json:"next_look"`
}

// loopActing says a spell of acting without progress has begun: logged,
// and told to the service (Config.OnLoopActing).
func (c *Client) loopActing(ctx context.Context, l dueLook, sent []string, after lookedAs) {
	acting := LoopActing{
		Service: c.reg.Service, Aggregate: l.aggregate, Namespace: l.namespace, ID: l.id,
		Looks: after.state.Acting, Commands: sent, At: after.state.Looked, NextLook: after.next,
	}
	c.log.WarnContext(ctx, "loop keeps acting without progress: its looks send the same commands and nothing recorded about the instance changes, so they are backed off until something does",
		"aggregate", l.aggregate, "namespace", l.namespace, "id", l.id, "looks", acting.Looks, "commands", sent, "next_look", after.next)
	if c.onLoopActing != nil {
		c.callHook(ctx, "OnLoopActing", func() { c.onLoopActing(ctx, acting) })
	}
}

// LoopDue is what the runtime holds about one looping instance.
type LoopDue struct {
	Aggregate string    `json:"aggregate"`
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
	// NextLook is when it is next looked at.
	NextLook time.Time `json:"next_look"`
	// Parent is the instance's supervisor, once a look has read it from
	// the state (@supervised_by); RestartedAt and RestartedBy are its
	// last restart by that parent (RestartChild).
	Parent      *ChildRef  `json:"parent,omitempty"`
	RestartedAt *time.Time `json:"restarted_at,omitempty"`
	RestartedBy string     `json:"restarted_by,omitempty"`
	LoopState
}

// LoopDue reads when an instance of a @loop aggregate is next looked
// at, and what has been noted about it since its last look. ok is false
// for an instance with no looks due: never created, or done.
func (c *Client) LoopDue(ctx context.Context, aggregate, namespace string, id uuid.UUID) (due LoopDue, ok bool, err error) {
	var (
		parentType *string
		parentID   *uuid.UUID
	)
	var (
		looked, event, outside *time.Time
		waits                  []byte
	)
	err = c.db.QueryRow(ctx, `
		SELECT next_look, looked_at, event_at, outside_at, acting, acting_sent, waits, failures, last_error, parked, parent_type, parent_id, restarted_at, restarted_by FROM loom_loop_due
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
		c.reg.Service, aggregate, namespace, id).Scan(&due.NextLook, &looked, &event, &outside, &due.Acting, &due.Sent, &waits, &due.Failures, &due.LastError, &due.Parked, &parentType, &parentID, &due.RestartedAt, &due.RestartedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return due, false, nil
	}
	if err != nil {
		return due, false, err
	}
	due.Aggregate, due.Namespace, due.ID = aggregate, namespace, id
	if parentType != nil && parentID != nil {
		due.Parent = &ChildRef{Aggregate: *parentType, Namespace: namespace, ID: *parentID}
	}
	if looked != nil {
		due.Looked = *looked
	}
	if event != nil {
		due.Event = *event
	}
	if outside != nil {
		due.Outside = *outside
	}
	_ = json.Unmarshal(waits, &due.Waits)
	return due, true, nil
}
