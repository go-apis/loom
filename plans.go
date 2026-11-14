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
	// Events is what the command's handler decided it would emit, in
	// order, each with what the schema says reacts to it. Nothing was
	// written and no reaction was run: a plan names who reacts and what
	// each is declared to dispatch and call, not what it would decide.
	Events    []PlannedEvent `json:"events"`
	ExpiresAt time.Time      `json:"expires_at"`
}

// PlannedEvent is one event a planned command would emit.
type PlannedEvent struct {
	Type string `json:"type"`
	Data any    `json:"data"`
	// Published is whether the event goes to the bus: other services
	// may consume it, and this service's schema cannot say which.
	Published bool `json:"published"`
	// Reactions is what this service's schema says reacts to the event
	// (Client.Reactions): who, not what they would decide.
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

// Plan decides cmd against the state as it stands, writes nothing of
// what it decided, and keeps the command for RunPlan. A command its
// handler refuses is refused here, with the handler's error: there is
// nothing to plan. ttl is how long the plan stays runnable; zero means
// DefaultPlanTTL.
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
	payloads, emits, version, err := c.decide(ctx, cmd)
	if err != nil {
		return nil, err
	}
	plan := &Plan{ID: uuid.New(), Command: name, Namespace: ns, TargetID: id, Version: version, Events: []PlannedEvent{}, ExpiresAt: nowUTC().Add(ttl)}
	for _, payload := range payloads {
		evt, _, err := c.eventNameOf(payload)
		if err != nil {
			return nil, err
		}
		if !contains(emits, evt) {
			return nil, &ContractError{Command: name, Emitted: evt}
		}
		def := c.reg.eventDef(evt)
		plan.Events = append(plan.Events, PlannedEvent{Type: evt, Data: payload, Published: def != nil && def.Publish, Reactions: c.Reactions(evt)})
	}

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

// decide runs a command's handler against the stored state and returns
// what it would emit, the events its contract allows, and the version
// the state was at. It writes nothing and takes no lock.
func (c *Client) decide(ctx context.Context, cmd Command) (payloads []any, emits []string, version int, err error) {
	name := cmd.LoomCommand()
	ns, id := cmd.CommandTarget()
	if agg, def := c.reg.aggregateForCommand(name); def != nil {
		state, v, err := c.loadState(ctx, c.db, agg, ns, id)
		if err != nil {
			return nil, nil, 0, err
		}
		payloads, err = def.Handle(ctx, state, cmd)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("%s: %w", name, err)
		}
		return payloads, def.Emits, v, nil
	}
	rec, def := c.reg.recordForCommand(name)
	if def == nil {
		return nil, nil, 0, fmt.Errorf("loom: nothing handles command %s", name)
	}
	state, err := c.record(ctx, c.db, rec.Name, ns, id)
	if err != nil {
		return nil, nil, 0, err
	}
	if state == nil {
		state = rec.NewState()
	}
	err = c.db.QueryRow(ctx, `
		SELECT version FROM loom_records
		WHERE service=$1 AND namespace=$2 AND record_type=$3 AND id=$4`,
		c.reg.Service, ns, rec.Name, id).Scan(&version)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, 0, err
	}
	payloads, err = def.Handle(ctx, state, cmd)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%s: %w", name, err)
	}
	return payloads, def.Emits, version, nil
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
