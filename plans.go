package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// A plan is a command held back: decided, shown, and not yet run. Plan
// works out what the command would do against the state as it stands
// and keeps the command; RunPlan runs exactly that command, once, and
// only while what it was planned against has not moved. It is how a
// caller sees a destructive command's effect before causing it — the
// gateway asks for one in place of dispatching a command the schema
// marks @plan.

// DefaultPlanTTL is how long a plan stays runnable when Plan is given
// no lifetime.
const DefaultPlanTTL = 15 * time.Minute

// Plan is a command held for a later RunPlan, and what it would do.
type Plan struct {
	ID        uuid.UUID `json:"id"`
	Command   string    `json:"command"`
	Namespace string    `json:"namespace"`
	// TargetID is the aggregate or record the command addresses, and
	// Version its version when the plan was made: RunPlan refuses once
	// it has moved.
	TargetID uuid.UUID `json:"target_id"`
	Version  int       `json:"version"`
	// Events is every event running the command would lead to, in the
	// order they would be written: the command's own first, then those
	// of the commands its reactions decide on, each naming what it came
	// from. The plan ran the command, its policies and its processes on
	// a transaction it then rolled back, so each reaction says what it
	// decided (PlannedReaction.Outcome). No outside call was made and
	// nothing was written.
	Events    []PlannedEvent `json:"events"`
	ExpiresAt time.Time      `json:"expires_at"`
}

// PlannedEvent is one event running a planned command would lead to.
type PlannedEvent struct {
	Type string `json:"type"`
	Data any    `json:"data"`
	// Aggregate and AggregateID are the stream the event would be
	// written to: the planned command's target, or that of a command a
	// reaction decided on.
	Aggregate   string    `json:"aggregate"`
	AggregateID uuid.UUID `json:"aggregate_id"`
	// Command is the command whose handler would emit the event, and
	// Cause the reaction that would dispatch that command — "policy
	// postLedger", "process coolAnvil" — or empty for the planned
	// command itself.
	Command string `json:"command"`
	Cause   string `json:"cause,omitempty"`
	// Published is whether the event goes to the bus: other services
	// may consume it, and this service's schema cannot say which.
	Published bool `json:"published"`
	// Reactions is what reacts to the event in this service
	// (Client.Reactions), each with what it decided when the plan ran
	// it.
	Reactions []PlannedReaction `json:"reactions"`
}

// ErrNoPlan is a plan id that names nothing runnable here: never made,
// made by another service, already run, or expired.
var ErrNoPlan = errors.New("loom: no such plan: it was never made here, was already run, or has expired")

// StalePlanError is a plan whose target changed after it was made. The
// plan is spent; plan again to see what the command would do now.
type StalePlanError struct {
	Command string
	Planned int
	Current int
}

func (e *StalePlanError) Error() string {
	return fmt.Sprintf("loom: the plan for %s is stale: its target was at version %d when planned and is at %d now — plan again", e.Command, e.Planned, e.Current)
}

// Plan runs cmd against the state as it stands on a transaction it
// rolls back, and keeps the command for RunPlan. What it answers is
// what the run would lead to: the command's events, what each policy
// would dispatch in the same transaction and the events of that, and
// what each process would then decide, followed the same way. A process
// is followed until it would call outside (loom.Once): the call is not
// made, and the plan says which call it stopped at. A command its
// handler refuses is refused here, with the handler's error, as is one
// a policy or a policy's command would fail: there is nothing to plan.
// ttl is how long the plan stays runnable; zero means DefaultPlanTTL.
//
// RunPlan checks that the command's own target has not moved, not
// everything its reactions read: what they decide at the run may differ
// from what they decided here.
func (c *Client) Plan(ctx context.Context, cmd Command, ttl time.Duration) (*Plan, error) {
	if v, ok := cmd.(Validator); ok {
		if err := v.Validate(); err != nil {
			return nil, fmt.Errorf("loom: %s: %w", cmd.LoomCommand(), err)
		}
	}
	if ttl <= 0 {
		ttl = DefaultPlanTTL
	}
	name := cmd.LoomCommand()
	ns, id := cmd.CommandTarget()
	if ns == "" {
		return nil, fmt.Errorf("loom: command %s has no namespace", name)
	}
	var (
		events  []PlannedEvent
		version int
		err     error
	)
	// a writer that lands on the same stream mid-plan is a conflict, as
	// for a dispatch: plan again against what it wrote
	for attempt := 0; attempt <= c.retries; attempt++ {
		events, version, err = c.planOnce(ctx, cmd)
		var conflict *ConflictError
		if !errors.As(err, &conflict) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	plan := &Plan{ID: uuid.New(), Command: name, Namespace: ns, TargetID: id, Version: version, Events: events, ExpiresAt: nowUTC().Add(ttl)}

	raw, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	// the command rests sealed, as one a timer holds does
	if raw, err = c.sealCommand(ctx, c.db, cmd, raw); err != nil {
		return nil, err
	}
	meta, err := json.Marshal(MetaFrom(ctx))
	if err != nil {
		return nil, err
	}
	// plans past their lifetime are never run (heldPlan); clearing them
	// here keeps the table from growing without a runner of its own
	if _, err := c.db.Exec(ctx, `DELETE FROM loom_plans WHERE service=$1 AND expires_at < now()`, c.reg.Service); err != nil {
		return nil, err
	}
	_, err = c.db.Exec(ctx, `
		INSERT INTO loom_plans (service, id, namespace, command_type, command, meta, target_id, version, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		c.reg.Service, plan.ID, ns, name, raw, meta, id, version, plan.ExpiresAt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// planBudget is how long a plan spends running processes. A process
// reads; one that waits on something else is left unfollowed rather
// than waited for.
const planBudget = 2 * time.Second

// maxPlanHops is how many processes deep a plan follows: a process's
// command, the process that reacts to its event, and so on.
const maxPlanHops = 10

// planOnce runs the command's unit of work, then its processes, on one
// transaction and rolls it back.
func (c *Client) planOnce(ctx context.Context, cmd Command) ([]PlannedEvent, int, error) {
	meta := MetaFrom(ctx)
	if meta.CorrelationID == "" {
		meta.CorrelationID = uuid.NewString()
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	// rolled back whatever ctx has come to: nothing a plan wrote stays
	defer tx.Rollback(context.WithoutCancel(ctx))

	run := &planRun{c: c}
	ctx = context.WithValue(ctx, planKey{}, run)
	ctx = withTxReader(ctx, c, tx)

	version, err := c.targetVersion(ctx, tx, cmd)
	if err != nil {
		return nil, 0, err
	}
	if _, _, err := c.runUnit(ctx, tx, []Command{cmd}, meta, &planScope{run: run}); err != nil {
		return nil, 0, err
	}
	run.processes(ctx, tx)
	return run.planned(), version, nil
}

// targetVersion is the version a command's target is at: what RunPlan
// must still find.
func (c *Client) targetVersion(ctx context.Context, q dbConn, cmd Command) (version int, err error) {
	name := cmd.LoomCommand()
	ns, id := cmd.CommandTarget()
	if agg, def := c.reg.aggregateForCommand(name); def != nil {
		_, version, err = c.loadState(ctx, q, agg, ns, id)
		return version, err
	}
	rec, def := c.reg.recordForCommand(name)
	if def == nil {
		return 0, fmt.Errorf("loom: nothing handles command %s", name)
	}
	err = q.QueryRow(ctx, `
		SELECT version FROM loom_records
		WHERE service=$1 AND namespace=$2 AND record_type=$3 AND id=$4`,
		c.reg.Service, ns, rec.Name, id).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

// planKey marks a context as a plan's: what runs under it is being
// worked out, not done.
type planKey struct{}

// Planning reports whether ctx belongs to a plan: the handler or
// reaction is being run to see what it would decide, on a transaction
// that will be rolled back. loom.Once makes no call under it. Code that
// does anything else outside the transaction checks this first.
func Planning(ctx context.Context) bool {
	_, ok := ctx.Value(planKey{}).(*planRun)
	return ok
}

// planRun is one plan being worked out: the events so far, in the order
// they would be written.
type planRun struct {
	c      *Client
	events []*plannedEvent
	// spent is set once the plan stops running processes: out of time,
	// or its transaction lost
	spent string
}

// plannedEvent is a PlannedEvent with the event it came from and how
// many processes deep it is.
type plannedEvent struct {
	PlannedEvent
	evt  *Event
	hops int
}

// planScope is what a unit of work records under while a plan runs it:
// the reaction whose commands these are, and how many processes deep.
// A nil scope is a dispatch, which records nothing.
type planScope struct {
	run  *planRun
	by   string
	hops int
}

// emitted records an event the unit wrote. by is the policy whose
// command emitted it, when it was not one of the unit's own.
func (s *planScope) emitted(evt *Event, cmd Command, by string) *plannedEvent {
	if s == nil {
		return nil
	}
	if by == "" {
		by = s.by
	}
	def := s.run.c.reg.eventDef(evt.Type)
	e := &plannedEvent{evt: evt, hops: s.hops, PlannedEvent: PlannedEvent{
		Type: evt.Type, Data: evt.Data,
		Aggregate: evt.AggregateType, AggregateID: evt.AggregateID,
		Command: cmd.LoomCommand(), Cause: by,
		Published: def != nil && def.Publish,
		Reactions: s.run.c.Reactions(evt.Type),
	}}
	s.run.events = append(s.run.events, e)
	return e
}

// reaction is the event's reaction of that kind and name, as the schema
// listed it.
func (e *plannedEvent) reaction(kind, name string) *PlannedReaction {
	for i := range e.Reactions {
		if e.Reactions[i].Kind == kind && e.Reactions[i].Name == name {
			return &e.Reactions[i]
		}
	}
	return nil
}

// decided records the commands a reaction answered the event with.
func (e *plannedEvent) decided(kind, name string, cmds []Command) {
	if e == nil {
		return
	}
	if r := e.reaction(kind, name); r != nil {
		r.Outcome = OutcomeDecided
		r.Commands = plannedDispatches(cmds)
	}
}

// planned is the plan's events as it answers them.
func (r *planRun) planned() []PlannedEvent {
	out := make([]PlannedEvent, len(r.events))
	for i, e := range r.events {
		out[i] = e.PlannedEvent
	}
	return out
}

// processes runs each process on each of the plan's own events, in the
// order the events were written, and the unit of work of whatever it
// decides to dispatch; the events of that are reacted to in their turn.
// A real process runs after the commit, beside the others; here each
// runs alone, on the plan's transaction, seeing what the plan has
// written so far.
func (r *planRun) processes(ctx context.Context, tx pgx.Tx) {
	deadline := time.Now().Add(planBudget)
	for i := 0; i < len(r.events); i++ {
		e := r.events[i]
		for _, p := range r.c.sortedReactors(r.c.reg.Processes) {
			pr := e.reaction("process", p.Name)
			if pr == nil {
				continue
			}
			switch {
			case r.spent != "":
				pr.Outcome, pr.Detail = OutcomeNotFollowed, r.spent
			case e.hops >= maxPlanHops:
				pr.Outcome, pr.Detail = OutcomeNotFollowed, fmt.Sprintf("the plan follows processes %d deep, and this is deeper", maxPlanHops)
			case !time.Now().Before(deadline):
				r.spent = fmt.Sprintf("the plan ran processes for %s and stopped before this one", planBudget)
				pr.Outcome, pr.Detail = OutcomeNotFollowed, r.spent
			default:
				r.process(ctx, tx, deadline, e, p, pr)
			}
		}
	}
}

// process runs one process on one planned event under a savepoint, so a
// reaction that fails, or whose commands are refused, leaves nothing of
// itself in what the reactions after it read.
func (r *planRun) process(ctx context.Context, tx pgx.Tx, deadline time.Time, e *plannedEvent, p *ReactorDef, pr *PlannedReaction) {
	c := r.c
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	// lost reports a transaction that can run nothing more: the plan
	// keeps what it has and follows no further
	lost := func(err error) bool {
		if ctx.Err() == nil && !tx.Conn().IsClosed() {
			return false
		}
		r.spent = fmt.Sprintf("the plan ran processes for %s and stopped at %s", planBudget, p.Name)
		pr.Outcome, pr.Detail, pr.Commands = OutcomeNotFollowed, r.spent, nil
		return true
	}
	sp, err := tx.Begin(ctx)
	if err != nil {
		if !lost(err) {
			pr.Outcome, pr.Detail = OutcomeNotFollowed, err.Error()
		}
		return
	}
	undo := func() { _ = sp.Rollback(context.WithoutCancel(ctx)) }

	effects := &plannedEffects{}
	rctx := withTxReader(ctx, c, sp)
	rctx = context.WithValue(rctx, effectScopeKey{}, &effectScope{c: c, p: p, plan: effects})
	cmds, err := p.React(rctx, e.evt)
	switch {
	case len(effects.reached) > 0:
		// whatever it answered, it answered without the call's result
		undo()
		pr.Outcome, pr.Calls = OutcomeCalls, effects.reached
		pr.Detail = "what it does next depends on what the call answers, and a plan does not make the call"
		return
	case err != nil:
		undo()
		switch {
		case lost(err):
		case IsNotYet(err):
			pr.Outcome, pr.Detail = OutcomeNotYet, err.Error()
		default:
			pr.Outcome, pr.Detail = OutcomeFails, err.Error()
		}
		return
	}
	pr.Outcome, pr.Commands = OutcomeDecided, plannedDispatches(cmds)
	if len(cmds) == 0 {
		undo()
		return
	}
	// its commands are a unit of work of their own, as react dispatches
	// them (runner.go)
	meta := Metadata{
		CorrelationID: e.evt.Meta.CorrelationID,
		CausationID:   fmt.Sprintf("evt:%s:%d", e.evt.Service, e.evt.GlobalSeq),
		Actor:         e.evt.Meta.Actor,
	}
	before := len(r.events)
	if _, _, err := c.runUnit(rctx, sp, cmds, meta, &planScope{run: r, by: "process " + p.Name, hops: e.hops + 1}); err != nil {
		undo()
		r.events = r.events[:before]
		if !lost(err) {
			pr.Outcome, pr.Detail = OutcomeFails, "what it dispatches is refused: "+err.Error()
		}
		return
	}
	if err := sp.Commit(ctx); err != nil {
		r.events = r.events[:before]
		if !lost(err) {
			pr.Outcome, pr.Detail = OutcomeFails, err.Error()
		}
	}
}

// plannedEffects is the outside calls a process reached while a plan
// ran it. None was made.
type plannedEffects struct {
	reached []string
}

// errPlannedEffect is what loom.Once answers under a plan in place of
// making the call.
type errPlannedEffect struct{ key string }

func (e *errPlannedEffect) Error() string {
	return fmt.Sprintf("loom: effect %s is not called while planning", e.key)
}

// plannedDispatches is the commands a reaction answered with, as a plan
// shows them.
func plannedDispatches(cmds []Command) []PlannedDispatch {
	out := []PlannedDispatch{}
	one := func(cmd Command, how string) PlannedDispatch {
		ns, id := cmd.CommandTarget()
		return PlannedDispatch{Command: cmd.LoomCommand(), Data: cmd, Namespace: ns, TargetID: id, How: how}
	}
	for _, cmd := range cmds {
		switch w := cmd.(type) {
		case *scheduledCommand:
			d := one(w.Command, DispatchTimer)
			at := w.at
			d.At = &at
			out = append(out, d)
		case *cancelTimer:
			out = append(out, one(w.Command, DispatchCancelTimer))
		case *asBatch:
			for _, b := range w.cmds {
				out = append(out, one(b, DispatchBatch))
			}
		default:
			out = append(out, one(cmd, DispatchRun))
		}
	}
	return out
}

// PlannedCommand is the command a plan holds, without running or
// spending it — what a gateway reads to decide whether this caller may
// run it. ErrNoPlan when the id names nothing runnable in namespace.
func (c *Client) PlannedCommand(ctx context.Context, namespace string, id uuid.UUID) (Command, error) {
	cmd, _, _, err := c.heldPlan(ctx, c.db, namespace, id, false)
	return cmd, err
}

// RunPlan runs the command a plan holds. The plan is spent whatever
// happens next: a second RunPlan of the same id is ErrNoPlan. The
// command runs only if its target is still at the version it was
// planned against (StalePlanError otherwise), so what runs is what the
// plan showed.
func (c *Client) RunPlan(ctx context.Context, namespace string, id uuid.UUID) error {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	cmd, version, meta, err := c.heldPlan(ctx, tx, namespace, id, true)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM loom_plans WHERE service=$1 AND id=$2`, c.reg.Service, id); err != nil {
		return err
	}
	// spent before it runs: a plan that fails halfway is planned again,
	// never run twice
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// the run is the runner's act and the planner's intent: whoever
	// runs it is the actor, where it names one, and the plan it came
	// from is the cause; the correlation is the planner's unless the
	// run brings its own
	now := MetaFrom(ctx)
	if now.Actor == "" {
		now.Actor = meta.Actor
	}
	if now.CorrelationID == "" {
		now.CorrelationID = meta.CorrelationID
	}
	if now.CausationID == "" {
		now.CausationID = "plan:" + id.String()
	}
	ctx = WithMeta(ctx, now)
	return c.Dispatch(context.WithValue(ctx, plannedKey{}, planned{cmd: cmd, version: version}), cmd)
}

// heldPlan reads a live plan's command, the version it was planned
// against and the metadata it was planned under. lock holds the row for
// the transaction, so two RunPlans of one id cannot both find it.
func (c *Client) heldPlan(ctx context.Context, q dbConn, namespace string, id uuid.UUID, lock bool) (Command, int, Metadata, error) {
	var (
		cmdType      string
		raw, metaRaw []byte
		version      int
		expires      time.Time
		meta         Metadata
	)
	sql := `
		SELECT command_type, command, meta, version, expires_at FROM loom_plans
		WHERE service=$1 AND namespace=$2 AND id=$3`
	if lock {
		sql += ` FOR UPDATE`
	}
	err := q.QueryRow(ctx, sql, c.reg.Service, namespace, id).Scan(&cmdType, &raw, &metaRaw, &version, &expires)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !nowUTC().Before(expires)) {
		return nil, 0, meta, ErrNoPlan
	}
	if err != nil {
		return nil, 0, meta, err
	}
	if raw, err = c.openCommand(ctx, q, cmdType, raw); err != nil {
		return nil, 0, meta, err
	}
	cmd, err := c.decodeCommand(cmdType, raw)
	if err != nil {
		return nil, 0, meta, err
	}
	_ = json.Unmarshal(metaRaw, &meta)
	return cmd, version, meta, nil
}

// plannedKey carries the command RunPlan is dispatching and the version
// it must find, to the handler that loads its state.
type plannedKey struct{}

type planned struct {
	cmd     Command
	version int
}

// stalePlan is the check a command handler makes once it has loaded
// the state under its transaction: a planned command runs only against
// the version it was planned at.
func stalePlan(ctx context.Context, cmd Command, current int) error {
	p, ok := ctx.Value(plannedKey{}).(planned)
	if !ok || p.cmd != cmd || p.version == current {
		return nil
	}
	return &StalePlanError{Command: cmd.LoomCommand(), Planned: p.version, Current: current}
}
