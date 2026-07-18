package graphql

import (
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/source"
)

// DefaultMaxDepth bounds query nesting when Config.MaxDepth is zero.
// Deep enough for GraphiQL's introspection query (~13 levels) with
// headroom, shallow enough that cyclic declared joins can't be wound
// into an amplification attack.
const DefaultMaxDepth = 20

// queryDepth measures the deepest field nesting in the request,
// following fragment spreads (cycle-guarded — cyclic fragments are a
// validation error later, they just don't hang us here). A query that
// fails to parse reports depth 0: the executor owns syntax errors.
func queryDepth(query string) int {
	doc, err := parser.Parse(parser.ParseParams{
		Source: source.NewSource(&source.Source{Body: []byte(query)}),
	})
	if err != nil {
		return 0
	}
	frags := map[string]*ast.FragmentDefinition{}
	for _, def := range doc.Definitions {
		if f, ok := def.(*ast.FragmentDefinition); ok && f.Name != nil {
			frags[f.Name.Value] = f
		}
	}
	var setDepth func(ss *ast.SelectionSet, seen map[string]bool) int
	setDepth = func(ss *ast.SelectionSet, seen map[string]bool) int {
		if ss == nil {
			return 0
		}
		max := 0
		for _, sel := range ss.Selections {
			d := 0
			switch s := sel.(type) {
			case *ast.Field:
				d = 1 + setDepth(s.SelectionSet, seen)
			case *ast.InlineFragment:
				d = setDepth(s.SelectionSet, seen)
			case *ast.FragmentSpread:
				if s.Name == nil || seen[s.Name.Value] {
					continue
				}
				if f := frags[s.Name.Value]; f != nil {
					seen[s.Name.Value] = true
					d = setDepth(f.SelectionSet, seen)
					delete(seen, s.Name.Value)
				}
			}
			if d > max {
				max = d
			}
		}
		return max
	}
	max := 0
	for _, def := range doc.Definitions {
		if op, ok := def.(*ast.OperationDefinition); ok {
			if d := setDepth(op.SelectionSet, map[string]bool{}); d > max {
				max = d
			}
		}
	}
	return max
}
