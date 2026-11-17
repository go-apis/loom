package graphql

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// Forking served (Config.Fork): forkStream re-runs an instance from a
// step in another namespace (loom.Client.ForkStream), compareFork reads
// the re-run against its source (loom.Client.CompareFork). Both are
// operator tools, like serviceStatus: a fork reads one namespace and
// writes another, so the decision names no namespace, and under
// DefaultPolicy only All access passes it (with Mutate, to fork).

const (
	forkField    = "forkStream"
	compareField = "compareFork"
)

var (
	forkResult = gql.NewObject(gql.ObjectConfig{
		Name:        "ForkResult",
		Description: "What a fork copied.",
		Fields: gql.Fields{
			"namespace": {Type: gql.NewNonNull(gql.String), Resolve: mapField("namespace")},
			"id":        {Type: gql.NewNonNull(scalarUUID), Resolve: mapField("id")},
			"version":   {Type: gql.NewNonNull(gql.Int), Resolve: mapField("version"), Description: "The fork's version: the last event copied."},
			"events":    {Type: gql.NewNonNull(gql.Int), Resolve: mapField("events"), Description: "How many events were copied."},
			"effects":   {Type: gql.NewNonNull(gql.Int), Resolve: mapField("effects"), Description: "How many recorded effects were copied, to be replayed instead of called again."},
		},
	})
	forkStep = gql.NewObject(gql.ObjectConfig{
		Name:        "ForkStep",
		Description: "One event a run wrote after the fork point.",
		Fields: gql.Fields{
			"version": {Type: gql.NewNonNull(gql.Int), Resolve: mapField("version")},
			"command": {Type: gql.NewNonNull(gql.String), Resolve: mapField("command"), Description: "The command whose handling wrote it."},
			"event":   {Type: gql.NewNonNull(gql.String), Resolve: mapField("event")},
			"cause":   {Type: gql.NewNonNull(gql.String), Resolve: mapField("cause"), Description: "What issued the command: loop, evt, timer, plan; empty for a direct dispatch."},
			"data":    {Type: gql.NewNonNull(gql.String), Resolve: jsonFieldText("data"), Description: "The payload as JSON."},
		},
	})
	forkDifference = gql.NewObject(gql.ObjectConfig{
		Name:        "ForkDifference",
		Description: "A step where a fork and its source part.",
		Fields: gql.Fields{
			"step":   {Type: gql.NewNonNull(gql.Int), Resolve: mapField("step"), Description: "Counted from 1 after the fork point."},
			"what":   {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))), Resolve: mapField("what"), Description: "command, event, data, or missing."},
			"source": {Type: forkStep, Resolve: mapField("source")},
			"fork":   {Type: forkStep, Resolve: mapField("fork")},
		},
	})
	forkComparison = gql.NewObject(gql.ObjectConfig{
		Name:        "ForkComparison",
		Description: "A fork read against its source after the fork point.",
		Fields: gql.Fields{
			"forkedAt":    {Type: gql.NewNonNull(gql.Int), Resolve: mapField("forked_at"), Description: "The last version the fork copied."},
			"same":        {Type: gql.NewNonNull(gql.Boolean), Resolve: mapField("same"), Description: "The fork did what its source did."},
			"differences": {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(forkDifference))), Resolve: mapField("differences")},
			"source":      {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(forkStep))), Resolve: mapField("source")},
			"fork":        {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(forkStep))), Resolve: mapField("fork")},
		},
	})
)

// jsonFieldText answers a map field as its JSON text.
func jsonFieldText(key string) gql.FieldResolveFn {
	return func(p gql.ResolveParams) (any, error) {
		src, _ := p.Source.(map[string]any)
		raw, err := json.Marshal(src[key])
		return string(raw), err
	}
}

// asDoc turns a result into the map the fields resolve from.
func asDoc(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	return doc, json.Unmarshal(raw, &doc)
}

// forkFields adds forkStream and compareFork over the composed services.
func (b *builder) forkFields(services []*loom.Client) error {
	owner := func(service, aggregate string) (*loom.Client, error) {
		var found *loom.Client
		for _, cli := range services {
			reg := cli.Registry()
			if service != "" && reg.Service != service {
				continue
			}
			for _, a := range reg.Aggregates {
				if a.Name == aggregate {
					if found != nil {
						return nil, fmt.Errorf("%s is an aggregate of more than one service: name the service", aggregate)
					}
					found = cli
				}
			}
		}
		if found == nil {
			return nil, fmt.Errorf("no service here has an aggregate %s", aggregate)
		}
		return found, nil
	}
	target := func(p gql.ResolveParams) (*loom.Client, string, string, uuid.UUID, string, error) {
		service, _ := p.Args["service"].(string)
		agg, _ := p.Args["aggregate"].(string)
		ns, _ := p.Args["namespace"].(string)
		to, _ := p.Args["toNamespace"].(string)
		id, err := uuid.Parse(fmt.Sprint(p.Args["id"]))
		if err != nil {
			return nil, "", "", uuid.Nil, "", fmt.Errorf("bad id %q", p.Args["id"])
		}
		cli, err := owner(service, agg)
		return cli, agg, ns, id, to, err
	}
	args := func() gql.FieldConfigArgument {
		return gql.FieldConfigArgument{
			"service":     {Type: gql.String, Description: "The service the aggregate is of; needed only when two have one by that name."},
			"aggregate":   {Type: gql.NewNonNull(gql.String)},
			"namespace":   {Type: gql.NewNonNull(scalarNamespace), Description: "The source's namespace."},
			"id":          {Type: gql.NewNonNull(scalarUUID)},
			"toNamespace": {Type: gql.NewNonNull(scalarNamespace), Description: "The fork's namespace."},
		}
	}

	b.gated("mutation", gate{}, false, forkField)
	forkArgs := args()
	forkArgs["upTo"] = &gql.ArgumentConfig{Type: gql.Int, Description: "The last version copied: the step to re-run from is the next. Omit to copy the whole stream."}
	if _, dup := b.muts[forkField]; dup {
		return fmt.Errorf("graphql: mutation %q defined twice", forkField)
	}
	b.muts[forkField] = &gql.Field{
		Description: "Fork an instance to re-run it from a step: copy its events up to a version into another namespace, where the service carries on from there on the code deployed now. Recorded effects replay rather than call the outside world again. Operators only.",
		Type:        gql.NewNonNull(forkResult),
		Args:        forkArgs,
		Resolve: func(p gql.ResolveParams) (any, error) {
			if err := decide(p.Context, Decision{Kind: "mutation", Field: forkField, Args: p.Args}); err != nil {
				return nil, err
			}
			cli, agg, ns, id, to, err := target(p)
			if err != nil {
				return nil, err
			}
			upTo, _ := p.Args["upTo"].(int)
			res, err := cli.ForkStream(p.Context, loom.ForkOptions{Aggregate: agg, Namespace: ns, ID: id, UpTo: upTo, ToNamespace: to})
			if err != nil {
				return nil, err
			}
			return asDoc(res)
		},
	}

	b.gated("query", gate{}, false, compareField)
	cmpArgs := args()
	cmpArgs["ignore"] = &gql.ArgumentConfig{Type: gql.NewList(gql.NewNonNull(gql.String)), Description: "Payload fields left out of the comparison: times and fresh ids."}
	return b.addQuery(compareField, &gql.Field{
		Description: "Compare a fork with its source: the commands each issued and the events each wrote after the fork point, and where they differ. A recorded run re-run on changed code is a regression test of it. Operators only.",
		Type:        gql.NewNonNull(forkComparison),
		Args:        cmpArgs,
		Resolve: func(p gql.ResolveParams) (any, error) {
			if err := decide(p.Context, Decision{Kind: "query", Field: compareField, Args: p.Args}); err != nil {
				return nil, err
			}
			cli, agg, ns, id, to, err := target(p)
			if err != nil {
				return nil, err
			}
			var ignore []string
			if list, ok := p.Args["ignore"].([]any); ok {
				for _, f := range list {
					ignore = append(ignore, fmt.Sprint(f))
				}
			}
			cmp, err := cli.CompareFork(p.Context, loom.CompareOptions{Aggregate: agg, Namespace: ns, ID: id, ToNamespace: to, Ignore: ignore})
			if err != nil {
				return nil, err
			}
			doc, err := asDoc(cmp)
			if err != nil {
				return nil, err
			}
			doc.(map[string]any)["same"] = cmp.Same()
			return doc, nil
		},
	})
}
