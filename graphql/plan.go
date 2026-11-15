package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// A command the schema marks @plan is not run by its mutation. The
// mutation works out what running it would lead to and answers a Plan —
// its events, and what each reaction to them decides — and
// `runPlan(namespace, id)` runs that
// plan: the same command, once, and only while its target has not
// moved (loom.Client.Plan, RunPlan). Both steps answer to the command's
// own gate, each as its own Decision (Stage "plan", then "run"), so a
// Policy can let one caller plan what only another may run.

var (
	plannedDispatch = gql.NewObject(gql.ObjectConfig{
		Name:        "PlannedDispatch",
		Description: "One command a reaction decided to dispatch when the plan ran it.",
		Fields: gql.Fields{
			"command":   {Type: gql.NewNonNull(gql.String)},
			"data":      {Type: scalarMap},
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"targetId":  {Type: gql.NewNonNull(scalarUUID), Description: "What the command addresses."},
			"how":       {Type: gql.NewNonNull(gql.String), Description: "What becomes of it: run (handled at once; its events are in the plan), timer (held until `at`), cancel_timer (the pending timer for it is dropped) or batch (queued to run later). A plan follows only run."},
			"at":        {Type: scalarTime, Description: "When a timer would fire."},
		},
	})
	plannedReaction = gql.NewObject(gql.ObjectConfig{
		Name:        "PlannedReaction",
		Description: "One thing that reacts to a planned event: who, what the schema says it may dispatch and call, and — for a policy or process of the event's own service — what it decided when the plan ran it.",
		Fields: gql.Fields{
			"kind":       {Type: gql.NewNonNull(gql.String), Description: "policy (in the event's own transaction), process (afterwards) or projection (a read model)."},
			"name":       {Type: gql.NewNonNull(gql.String)},
			"service":    {Type: gql.String, Description: "The reactor's service, when it is another service's process consuming the event."},
			"dispatches": {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))), Description: "Commands the schema says it may dispatch in answer. It may dispatch any of them, or none."},
			"effects":    {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))), Description: "Outside calls a process declares. They are the process's, not this event's alone."},
			"entity":     {Type: gql.String, Description: "The read model a projection updates."},
			"outcome":    {Type: gql.String, Description: "What the plan found when it ran the reaction: decided (it ran to its end; `commands` is what it would dispatch), calls (a process reached an outside call, which a plan does not make, so what it does next is not known), fails (it returned an error, or what it dispatches is refused), not_yet (it would look again later) or not_followed (the plan did not run it). Null for what a plan does not run: a projection, another service's process."},
			"detail":     {Type: gql.String, Description: "More where the outcome is not the whole answer: the error, or why it was not followed."},
			"commands":   {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(plannedDispatch))), Description: "What the reaction decided to dispatch, in order. The events of those that run are in the plan's events, with this reaction as their cause."},
			"calls":      {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))), Description: "The outside call a process stopped at."},
		},
	})
	plannedEvent = gql.NewObject(gql.ObjectConfig{
		Name:        "PlannedEvent",
		Description: "One event running a planned command would lead to.",
		Fields: gql.Fields{
			"type":        {Type: gql.NewNonNull(gql.String)},
			"data":        {Type: scalarMap},
			"aggregate":   {Type: gql.NewNonNull(gql.String), Description: "The aggregate or record the event would be written to."},
			"aggregateId": {Type: gql.NewNonNull(scalarUUID)},
			"command":     {Type: gql.NewNonNull(gql.String), Description: "The command whose handler would emit it."},
			"cause":       {Type: gql.String, Description: "The reaction that would dispatch that command — \"policy postLedger\", \"process coolAnvil\". Null for the planned command itself."},
			"published":   {Type: gql.NewNonNull(gql.Boolean), Description: "Whether it goes to the bus, where services outside this gateway may react to it."},
			"reactions":   {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(plannedReaction))), Description: "What reacts to it: this service's policies, processes and projections, then other services' processes on this gateway."},
		},
	})
	planObject = gql.NewObject(gql.ObjectConfig{
		Name:        "Plan",
		Description: "A command held back: what it would do, and the id that runs it (runPlan). Nothing has changed yet.",
		Fields: gql.Fields{
			"id":        {Type: gql.NewNonNull(scalarUUID), Description: "Pass this to runPlan to run the command."},
			"command":   {Type: gql.NewNonNull(gql.String)},
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"targetId":  {Type: gql.NewNonNull(scalarUUID), Description: "What the command addresses."},
			"version":   {Type: gql.NewNonNull(gql.Int), Description: "The target's version when planned; the plan is stale once it moves."},
			"events":    {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(plannedEvent))), Description: "Every event running the command would lead to, in the order they would be written: its own, then those of the commands its reactions decide on. The plan ran the command and its reactions and kept nothing; no outside call was made. What the reactions decide at the run may differ if what they read has changed by then."},
			"expiresAt": {Type: gql.NewNonNull(scalarTime)},
		},
	})
)

// plannedOp is a @plan command as runPlan needs it: the service that
// holds its plans, its mutation field, its gate and its state.
type plannedOp struct {
	cli    *loom.Client
	field  string
	gate   gate
	stored stateLoader
}

// planDoc renders a plan. consumed answers, for one of its event types,
// the reactions of the gateway's other services: their processes that
// consume the event off the bus.
func planDoc(p *loom.Plan, consumed func(eventType string) []loom.PlannedReaction) (map[string]any, error) {
	object := func(v any) (map[string]any, error) {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		data := map[string]any{}
		_ = json.Unmarshal(raw, &data)
		return data, nil
	}
	events := make([]any, len(p.Events))
	for i, e := range p.Events {
		data, err := object(e.Data)
		if err != nil {
			return nil, err
		}
		reactions := []any{}
		for _, r := range append(append([]loom.PlannedReaction(nil), e.Reactions...), consumed(e.Type)...) {
			commands := []any{}
			for _, d := range r.Commands {
				given, err := object(d.Data)
				if err != nil {
					return nil, err
				}
				cmd := map[string]any{"command": d.Command, "data": given, "namespace": d.Namespace, "targetId": d.TargetID.String(), "how": d.How}
				if d.At != nil {
					cmd["at"] = *d.At
				}
				commands = append(commands, cmd)
			}
			doc := map[string]any{"kind": r.Kind, "name": r.Name, "dispatches": orEmpty(r.Dispatches), "effects": orEmpty(r.Effects), "commands": commands, "calls": orEmpty(r.Calls)}
			for field, value := range map[string]string{"service": r.Service, "entity": r.Entity, "outcome": r.Outcome, "detail": r.Detail} {
				if value != "" {
					doc[field] = value
				}
			}
			reactions = append(reactions, doc)
		}
		doc := map[string]any{
			"type": e.Type, "data": data, "aggregate": e.Aggregate, "aggregateId": e.AggregateID.String(),
			"command": e.Command, "published": e.Published, "reactions": reactions,
		}
		if e.Cause != "" {
			doc["cause"] = e.Cause
		}
		events[i] = doc
	}
	return map[string]any{
		"id": p.ID.String(), "command": p.Command, "namespace": p.Namespace,
		"targetId": p.TargetID.String(), "version": p.Version,
		"events": events, "expiresAt": p.ExpiresAt,
	}, nil
}

func orEmpty(list []string) []any {
	out := make([]any, len(list))
	for i, s := range list {
		out[i] = s
	}
	return out
}

// consumedBy answers what the gateway's other services declare they do
// with one of cli's events.
func (b *builder) consumedBy(cli *loom.Client) func(eventType string) []loom.PlannedReaction {
	return func(eventType string) []loom.PlannedReaction {
		var out []loom.PlannedReaction
		for _, other := range b.services {
			if other != cli {
				out = append(out, other.ConsumedReactions(cli.Registry().Service, eventType)...)
			}
		}
		return out
	}
}

// runPlanMutation adds `runPlan` once any composed service has a @plan
// command.
func (b *builder) runPlanMutation() {
	if len(b.planned) == 0 {
		return
	}
	const field = "runPlan"
	b.gated("mutation", gate{}, true, field)
	b.muts[field] = &gql.Field{
		Description: "Run a planned command by its plan's id: exactly the command that was planned, once, and only while what it was planned against is unchanged. A stale, spent or expired plan is refused — plan again.",
		Type:        gql.NewNonNull(dispatchResult),
		Args: gql.FieldConfigArgument{
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"id":        {Type: gql.NewNonNull(scalarUUID)},
		},
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns, _ := p.Args["namespace"].(string)
			id, err := uuid.Parse(fmt.Sprint(p.Args["id"]))
			if err != nil {
				return nil, fmt.Errorf("bad id %q", p.Args["id"])
			}
			// the namespace gate first, as for any mutation: a caller
			// with no access here learns nothing of what is held
			if err := decide(p.Context, Decision{Kind: "mutation", Field: field, Namespace: ns, Args: p.Args}); err != nil {
				return nil, err
			}
			cmd, op, err := b.heldCommand(p.Context, ns, id)
			if err != nil {
				return nil, err
			}
			// then the planned command's own gate, for THIS caller
			given := map[string]any{}
			if raw, err := json.Marshal(cmd); err == nil {
				_ = json.Unmarshal(raw, &given)
			}
			_, target := cmd.CommandTarget()
			d := Decision{Kind: "mutation", Field: op.field, Namespace: ns, Args: given, Roles: op.gate.roles, Rules: op.gate.rules, Stage: StageRun}
			if len(op.gate.rules) > 0 {
				d.Target = commandTarget(p.Context, op.gate.rules, ns, target, given, op.stored)
			}
			if err := decide(p.Context, d); err != nil {
				return nil, err
			}
			if err := op.cli.RunPlan(p.Context, ns, id); err != nil {
				return nil, err
			}
			return map[string]any{"status": "ok"}, nil
		},
	}
}

// heldCommand finds the plan among the composed services.
func (b *builder) heldCommand(ctx context.Context, ns string, id uuid.UUID) (loom.Command, plannedOp, error) {
	seen := map[*loom.Client]bool{}
	for _, op := range b.planned {
		if seen[op.cli] {
			continue
		}
		seen[op.cli] = true
		cmd, err := op.cli.PlannedCommand(ctx, ns, id)
		if errors.Is(err, loom.ErrNoPlan) {
			continue
		}
		if err != nil {
			return nil, plannedOp{}, err
		}
		held, ok := b.planned[cmd.LoomCommand()]
		if !ok {
			return nil, plannedOp{}, fmt.Errorf("graphql: plan %s holds %s, which this gateway does not plan", id, cmd.LoomCommand())
		}
		return cmd, held, nil
	}
	return nil, plannedOp{}, loom.ErrNoPlan
}
