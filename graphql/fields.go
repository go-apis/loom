package graphql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// A field's @allow says which callers are shown it. A caller no rule
// holds for still reads the record: the field resolves to null, and the
// response says so in its errors, at the field's path and under the code
// FieldHidden — so a client can tell "hidden from you" from "not set",
// which a bare null cannot say.
//
// This is the one place the gateway answers in part. A Policy that
// forbids a field still denies the whole operation (Decision.Fields);
// a declared field rule is the schema saying this field is routinely
// not everyone's, and a page written once for every caller should still
// load for the ones it is not for.
//
// Hiding the value is not enough: a caller that may filter or order a
// list by a hidden field can read it back one comparison at a time. So
// a list is refused when it filters or orders by a field hidden from
// its caller (queryFromArgs).

// FieldHidden is the `extensions.code` of the error a hidden field
// resolves with.
const FieldHidden = "FIELD_HIDDEN"

// hiddenError is what a field hidden from its caller resolves with.
type hiddenError struct {
	typ, field string
	rules      []loom.Rule
}

func (e *hiddenError) Error() string {
	return fmt.Sprintf("hidden: %s.%s is shown when %s", e.typ, e.field, shownWhen(e.rules))
}

// Extensions puts the code on the GraphQL error (gqlerrors.ExtendedError).
func (e *hiddenError) Extensions() map[string]any {
	return map[string]any{"code": FieldHidden}
}

// shownWhen reads field rules aloud: "kind is admin, or kind is staff".
func shownWhen(rules []loom.Rule) string {
	said := make([]string, len(rules))
	for i, r := range rules {
		attrs := make([]string, 0, len(r.Caller))
		for attr := range r.Caller {
			attrs = append(attrs, attr)
		}
		sort.Strings(attrs)
		parts := make([]string, len(attrs))
		for j, attr := range attrs {
			parts[j] = attr + " is " + r.Caller[attr]
		}
		said[i] = strings.Join(parts, " and ")
	}
	return strings.Join(said, ", or ")
}

// fieldShown reports whether the caller on ctx is shown a field with
// these rules. As with an operation's rules: an open mount shows
// everything, All access with no attribute model shows everything, and
// otherwise one rule must hold — so a scoped caller with no attributes
// is shown no ruled field.
func fieldShown(ctx context.Context, rules []loom.Rule) bool {
	if len(rules) == 0 {
		return true
	}
	a, has := AccessFrom(ctx)
	if !has || (a.All && a.Attrs == nil) {
		return true
	}
	for _, r := range rules {
		if callerMatches(a, r) {
			return true
		}
	}
	return false
}

// guarded is a field's resolver with its rules in front.
func guarded(typ, field string, rules []loom.Rule, resolve gql.FieldResolveFn) gql.FieldResolveFn {
	if len(rules) == 0 {
		return resolve
	}
	return func(p gql.ResolveParams) (any, error) {
		if !fieldShown(p.Context, rules) {
			return nil, &hiddenError{typ: typ, field: field, rules: rules}
		}
		return resolve(p)
	}
}

// registerFieldRules gathers the registry's per-field rules by type.
func (b *builder) registerFieldRules(reg *loom.Registry) {
	if b.fieldRules == nil {
		b.fieldRules = map[string]map[string][]loom.Rule{}
	}
	add := func(typ string, rules map[string][]loom.Rule) {
		if len(rules) > 0 {
			b.fieldRules[typ] = rules
		}
	}
	for _, a := range reg.Aggregates {
		add(a.Name, a.FieldAllow)
	}
	for _, r := range reg.Records {
		add(r.Name, r.FieldAllow)
	}
	for _, p := range reg.Projections {
		add(p.Entity, p.EntityFieldAllow)
	}
}

// hiddenListField is the error for a list that filters or orders by a
// field hidden from its caller, or nil.
func hiddenListField(ctx context.Context, field string, fields map[string][]loom.Rule, doing string) error {
	if rules := fields[field]; !fieldShown(ctx, rules) {
		return fmt.Errorf("access denied: cannot %s by %s, which is hidden from this caller (it is shown when %s)", doing, field, shownWhen(rules))
	}
	return nil
}

func (h *handler) Hidden(ctx context.Context, typ string) map[string]string {
	out := map[string]string{}
	for snake, rules := range h.fieldRules[typ] {
		if !fieldShown(ctx, rules) {
			out[camel(snake)] = shownWhen(rules)
		}
	}
	return out
}
