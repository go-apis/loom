package graphql

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// Approvals through the gateway. A service with a @loop aggregate can
// ask for approvals (loom/approval.go); here they are listed — what is
// waiting on whom — and answered, with no code of the service's own.
// `answerApproval` is a mutation like any other to the gateway's rules,
// and one more besides: the caller must hold the approval's role in the
// namespace (Access.Roles), or have every namespace with no role model
// at all. The caller's actor is recorded as who answered.

var approvalObject = gql.NewObject(gql.ObjectConfig{
	Name:        "Approval",
	Description: "A yes or no someone is waited on for: what is asked, the role that may answer, the deadline, and what happens at the deadline if nobody has.",
	Fields: gql.Fields{
		"service":    {Type: gql.NewNonNull(gql.String)},
		"aggregate":  {Type: gql.NewNonNull(gql.String), Description: "The aggregate whose instance asks."},
		"namespace":  {Type: gql.NewNonNull(scalarNamespace)},
		"id":         {Type: gql.NewNonNull(scalarUUID), Description: "The instance that asks."},
		"name":       {Type: gql.NewNonNull(gql.String)},
		"what":       {Type: gql.NewNonNull(gql.String), Description: "What is being approved."},
		"role":       {Type: gql.NewNonNull(gql.String), Description: "The role that may answer."},
		"deadline":   {Type: gql.NewNonNull(scalarTime)},
		"atDeadline": {Type: gql.NewNonNull(gql.String), Description: "What it becomes if nobody answers in time: approve, reject or expire."},
		"status":     {Type: gql.NewNonNull(gql.String), Description: "open, approved, rejected or expired."},
		"answeredBy": {Type: gql.String, Description: "Who answered, or \"deadline\"."},
		"reason":     {Type: gql.String},
		"askedAt":    {Type: gql.NewNonNull(scalarTime)},
		"answeredAt": {Type: scalarTime},
	},
})

func approvalDoc(service string, r loom.ApprovalRecord) map[string]any {
	doc := map[string]any{
		"service": service, "aggregate": r.Aggregate, "namespace": r.Namespace, "id": r.ID.String(), "name": r.Name,
		"what": r.What, "role": r.Role, "deadline": r.Deadline.UTC().Format(time.RFC3339Nano), "atDeadline": r.AtDeadline,
		"status": r.Status, "askedAt": r.AskedAt.UTC().Format(time.RFC3339Nano),
	}
	if r.Settled() {
		doc["answeredBy"] = r.AnsweredBy
		if r.Reason != "" {
			doc["reason"] = r.Reason
		}
		if r.AnsweredAt != nil {
			doc["answeredAt"] = r.AnsweredAt.UTC().Format(time.RFC3339Nano)
		}
	}
	return doc
}

// approvalFields adds the approval fields once any composed service has
// a @loop aggregate.
func (b *builder) approvalFields() error {
	looping := map[string]*loom.Client{} // aggregate → its service
	var with []*loom.Client
	for _, cli := range b.services {
		any := false
		for _, a := range cli.Registry().Aggregates {
			if a.Look != nil {
				looping[a.Name], any = cli, true
			}
		}
		if any {
			with = append(with, cli)
		}
	}
	if len(with) == 0 {
		return nil
	}
	b.gated("query", gate{}, true, "approvals")
	if err := b.addQuery("approvals", &gql.Field{
		Description: "Approvals in a namespace, those whose deadline is nearest first: what is waiting on whom. Open ones unless a status is given.",
		Type:        gql.NewNonNull(gql.NewList(gql.NewNonNull(approvalObject))),
		Args: gql.FieldConfigArgument{
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"status":    {Type: gql.String, Description: "open (the default), approved, rejected or expired."},
			"role":      {Type: gql.String, Description: "Only those this role may answer."},
			"limit":     {Type: gql.Int},
		},
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns, _ := p.Args["namespace"].(string)
			if err := decide(p.Context, Decision{Kind: "query", Field: "approvals", Namespace: ns, Args: p.Args}); err != nil {
				return nil, err
			}
			q := loom.ApprovalQuery{Namespace: ns}
			q.Status, _ = p.Args["status"].(string)
			q.Role, _ = p.Args["role"].(string)
			q.Limit, _ = p.Args["limit"].(int)
			out := []any{}
			for _, cli := range with {
				recs, err := cli.Approvals(p.Context, q)
				if err != nil {
					return nil, err
				}
				for _, r := range recs {
					out = append(out, approvalDoc(cli.Registry().Service, r))
				}
			}
			return out, nil
		},
	}); err != nil {
		return err
	}

	const field = "answerApproval"
	b.gated("mutation", gate{}, true, field)
	b.muts[field] = &gql.Field{
		Description: "Answer an open approval, yes or no. You must hold the role it asks for in the namespace. The answer is recorded as yours, and the instance that asked is told at once.",
		Type:        gql.NewNonNull(approvalObject),
		Args: gql.FieldConfigArgument{
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"aggregate": {Type: gql.NewNonNull(gql.String), Description: "The aggregate whose instance asks."},
			"id":        {Type: gql.NewNonNull(scalarUUID), Description: "The instance that asks."},
			"name":      {Type: gql.NewNonNull(gql.String)},
			"approve":   {Type: gql.NewNonNull(gql.Boolean)},
			"reason":    {Type: gql.String, Description: "Why: worth giving for a no."},
		},
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns, _ := p.Args["namespace"].(string)
			if err := decide(p.Context, Decision{Kind: "mutation", Field: field, Namespace: ns, Args: p.Args}); err != nil {
				return nil, err
			}
			aggregate, _ := p.Args["aggregate"].(string)
			name, _ := p.Args["name"].(string)
			id, err := uuid.Parse(fmt.Sprint(p.Args["id"]))
			if err != nil {
				return nil, fmt.Errorf("bad id %q", p.Args["id"])
			}
			cli := looping[aggregate]
			if cli == nil {
				return nil, fmt.Errorf("no aggregate %q asks for approvals", aggregate)
			}
			answer := loom.ApprovalAnswer{}
			answer.Approve, _ = p.Args["approve"].(bool)
			answer.Reason, _ = p.Args["reason"].(string)
			// the caller's roles in this namespace; every-namespace access
			// with no role model answers as whatever is asked for
			if access, ok := AccessFrom(p.Context); ok {
				for _, key := range []string{ns, "*"} {
					if role := access.Roles[key]; role != "" {
						answer.Roles = append(answer.Roles, role)
					}
				}
				if access.All && len(access.Roles) == 0 {
					if rec, err := cli.Approval(p.Context, aggregate, ns, id, name); err == nil && rec != nil {
						answer.Roles = []string{rec.Role}
					}
				}
			} else if rec, err := cli.Approval(p.Context, aggregate, ns, id, name); err == nil && rec != nil {
				answer.Roles = []string{rec.Role} // an open mount: no caller to hold a role
			}
			rec, err := cli.AnswerApproval(p.Context, aggregate, ns, id, name, answer)
			if err != nil {
				return nil, err
			}
			return approvalDoc(cli.Registry().Service, *rec), nil
		},
	}
	return nil
}
