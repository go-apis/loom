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
// mutation decides it against the state as it stands and answers a
// Plan — what it would emit — and `runPlan(namespace, id)` runs that
// plan: the same command, once, and only while its target has not
// moved (loom.Client.Plan, RunPlan). Both steps answer to the command's
// own gate, each as its own Decision (Stage "plan", then "run"), so a
// Policy can let one caller plan what only another may run.

var (
	plannedEvent = gql.NewObject(gql.ObjectConfig{
		Name:        "PlannedEvent",
		Description: "One event a planned command would emit.",
		Fields: gql.Fields{
			"type": {Type: gql.NewNonNull(gql.String)},
			"data": {Type: scalarMap},
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
			"events":    {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(plannedEvent))), Description: "What the command would emit, in order. What reacts to these events is not part of the plan."},
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

func planDoc(p *loom.Plan) (map[string]any, error) {
	events := make([]any, len(p.Events))
	for i, e := range p.Events {
		raw, err := json.Marshal(e.Data)
		if err != nil {
			return nil, err
		}
		data := map[string]any{}
		_ = json.Unmarshal(raw, &data)
		events[i] = map[string]any{"type": e.Type, "data": data}
	}
	return map[string]any{
		"id": p.ID.String(), "command": p.Command, "namespace": p.Namespace,
		"targetId": p.TargetID.String(), "version": p.Version,
		"events": events, "expiresAt": p.ExpiresAt,
	}, nil
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
