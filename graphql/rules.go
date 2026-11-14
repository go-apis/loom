package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
)

// gate is what the schema declares about who may run one generated
// operation: its @role names and its @allow rules.
type gate struct {
	roles []string
	rules []loom.Rule
}

// rootGate is a root field as a probe needs it: whether it is a query
// or a mutation, what the schema declares about who may run it, and
// whether it takes a namespace (a hand-written Field may not).
type rootGate struct {
	gate
	kind       string
	namespaced bool
}

// stateLoader reads the stored state of the thing an operation touches
// as a doc map (json field names), or nil when there is none yet.
type stateLoader func(ctx context.Context, ns string, id uuid.UUID) (map[string]any, error)

// checkRules is DefaultPolicy's @allow step. An operation with rules is
// allowed when one of them holds for the caller: every Caller pair
// equals the caller's attribute, and every Own field of the target
// equals the caller attribute it names. A list has no one target, so a
// rule holds for it on its Caller pairs alone and its Own pairs narrow
// the rows instead (ownFilters); a probe has no target either, and
// asks only whether a rule could hold.
//
// An open mount is not gated, as with roles. God access (All) with no
// attribute model keeps its meaning — everything; any Attrs map switches
// to strict rule checks. A scoped caller with no attributes meets no
// rule, so adding @allow to a schema fails closed until the Auth hook
// grants attributes.
func checkRules(d Decision) error {
	if len(d.Rules) == 0 || !d.HasAccess {
		return nil
	}
	a := d.Access
	if a.All && a.Attrs == nil {
		return nil
	}
	var target map[string]any
	loaded := false
	for _, r := range d.Rules {
		if !callerMatches(a, r) {
			continue
		}
		if len(r.Own) == 0 || d.List || d.Probe {
			return nil
		}
		if !loaded {
			loaded = true
			if d.Target != nil {
				t, err := d.Target()
				if err != nil {
					return err
				}
				target = t
			}
		}
		if owns(a, r, target) {
			return nil
		}
	}
	return fmt.Errorf("access denied: %s %q is not allowed for this caller", d.Kind, d.Field)
}

// callerMatches reports whether the caller has every attribute value
// the rule asks for, and holds every attribute its Own pairs compare
// against — a caller with no project owns no project's rows.
func callerMatches(a Access, r loom.Rule) bool {
	for attr, want := range r.Caller {
		if a.Attrs[attr] != want {
			return false
		}
	}
	for _, attr := range r.Own {
		if a.Attrs[attr] == "" {
			return false
		}
	}
	return true
}

func owns(a Access, r loom.Rule, target map[string]any) bool {
	for field, attr := range r.Own {
		have, ok := target[field]
		if !ok || blank(have) || fmt.Sprint(have) != a.Attrs[attr] {
			return false
		}
	}
	return true
}

// ownFilters is what a list is narrowed to for this caller: the Own
// pairs of the rule that lets it in, as equality filters. No filters
// when the operation has no rules, when a rule with no Own pairs lets
// the caller in (it may see every row), or when no rule matches — then
// a Policy allowed the list on grounds of its own and answers for it.
func ownFilters(ctx context.Context, rules []loom.Rule) []loom.Filter {
	a, has := AccessFrom(ctx)
	if len(rules) == 0 || !has || (a.All && a.Attrs == nil) {
		return nil
	}
	var narrow *loom.Rule
	for i := range rules {
		if !callerMatches(a, rules[i]) {
			continue
		}
		if len(rules[i].Own) == 0 {
			return nil
		}
		if narrow == nil {
			narrow = &rules[i]
		}
	}
	if narrow == nil {
		return nil
	}
	fields := make([]string, 0, len(narrow.Own))
	for f := range narrow.Own {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	out := make([]loom.Filter, len(fields))
	for i, f := range fields {
		out[i] = loom.Filter{Field: f, Op: "eq", Value: a.Attrs[narrow.Own[f]]}
	}
	return out
}

// commandTarget is the values a command's rules compare: each Own field
// as the stored state has it, and where the state has none (the thing
// does not exist yet, or never set the field) as the payload gives it.
// The state is the authority — a payload cannot claim another owner's
// thing — so a field the two disagree on is left out, and no rule owns
// it.
func commandTarget(ctx context.Context, rules []loom.Rule, args map[string]any, payload converter, load stateLoader) func() (map[string]any, error) {
	return func() (map[string]any, error) {
		ns, _ := args["namespace"].(string)
		id, err := uuid.Parse(fmt.Sprint(args["aggregateId"]))
		if err != nil {
			return nil, fmt.Errorf("bad aggregateId %q", args["aggregateId"])
		}
		state, err := load(ctx, ns, id)
		if err != nil {
			return nil, err
		}
		// the payload by snake names, as the schema and the rules write them
		given := map[string]any{}
		if raw, err := json.Marshal(payload(args)); err == nil {
			_ = json.Unmarshal(raw, &given)
		}
		out := map[string]any{}
		for _, r := range rules {
			for field := range r.Own {
				stored, sent := state[field], given[field]
				switch {
				case blank(stored):
					out[field] = sent
				case blank(sent) || fmt.Sprint(sent) == fmt.Sprint(stored):
					out[field] = stored
				}
			}
		}
		return out, nil
	}
}

// docTarget is a read's target: the stored doc itself.
func docTarget(ctx context.Context, ns string, id uuid.UUID, load stateLoader) func() (map[string]any, error) {
	return func() (map[string]any, error) { return load(ctx, ns, id) }
}

// blank reports a value no rule can own: absent, empty, or the zero id.
func blank(v any) bool {
	if v == nil {
		return true
	}
	s := fmt.Sprint(v)
	return s == "" || s == uuid.Nil.String()
}
