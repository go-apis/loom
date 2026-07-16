package extract

import (
	"go/ast"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// The body scans below are best-effort static analysis: they catch composite
// literals and calls one level into module helper functions, which covers
// the codebase's conventions. Dynamically built commands/events may be
// under-reported; the schema labels these fields accordingly.

// emits collects the events applied within an aggregate command handler:
// a.Apply(ctx, &events.X{...}) plus holder-style a.Publish(&events.X{...}),
// following one level of module callees.
func (e *extractor) emits(m method) []string {
	names := map[string]bool{}
	e.scanBody(m, 1, func(pkg *packages.Package, node ast.Node) {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		// events need no marker in this lib (plain structs are appliable), so
		// filter on the receiver instead: Apply on an aggregate, Publish on a
		// holder. That is exactly what reaches the store at runtime.
		recv := pkg.TypesInfo.TypeOf(sel.X)
		if recv == nil {
			return
		}
		var arg ast.Expr
		switch {
		case sel.Sel.Name == "Apply" && len(call.Args) == 2 && types.Implements(recv, e.ifaces["Aggregate"]):
			arg = call.Args[1]
		case sel.Sel.Name == "Publish" && len(call.Args) == 1 && types.Implements(recv, e.ifaces["AggregateHolder"]):
			arg = call.Args[0]
		default:
			return
		}
		n := concreteNamed(pkg.TypesInfo.TypeOf(arg))
		if n == nil || n.Obj().Pkg() == nil || n.Obj().Pkg() == e.es {
			return
		}
		names[e.registerEvent(n).Name] = true
	})
	return sortedKeys(names)
}

// dispatches collects the command types constructed within a saga handle,
// one level into module helpers (e.g. commands.MassPayoutPayoutCommands).
func (e *extractor) dispatches(m method) []string {
	names := map[string]bool{}
	e.scanBody(m, 1, func(pkg *packages.Package, node ast.Node) {
		lit, ok := node.(*ast.CompositeLit)
		if !ok {
			return
		}
		n := concreteNamed(pkg.TypesInfo.TypeOf(lit))
		if n == nil || n.Obj().Pkg() == nil || n.Obj().Pkg() == e.es {
			return
		}
		if !e.implementsCommand(n) {
			return
		}
		names[n.Obj().Name()] = true
	})
	return sortedKeys(names)
}

// chunkedDispatch reports whether the handle processes in chunks: a
// unit.Dispatch (or es.NewDetachedUnit) reachable inside a loop, directly or
// one call level down — the pattern behind the lib's detached-unit chunked
// commits.
func (e *extractor) chunkedDispatch(m method) bool {
	found := false
	e.scanBody(m, 1, func(pkg *packages.Package, node ast.Node) {
		switch loop := node.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			ast.Inspect(loop, func(inner ast.Node) bool {
				if found {
					return false
				}
				call, ok := inner.(*ast.CallExpr)
				if !ok {
					return true
				}
				if e.callDispatches(pkg, call, 2, map[*ast.FuncDecl]bool{}) {
					found = true
				}
				return true
			})
		}
	})
	return found
}

// callDispatches reports whether a call is a unit dispatch / detached-unit
// mint, or leads to one within depth levels of module callees.
func (e *extractor) callDispatches(pkg *packages.Package, call *ast.CallExpr, depth int, seen map[*ast.FuncDecl]bool) bool {
	var obj types.Object
	switch f := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		obj = pkg.TypesInfo.Uses[f]
	case *ast.SelectorExpr:
		obj = pkg.TypesInfo.Uses[f.Sel]
		if fn, ok := obj.(*types.Func); ok {
			if fn.Name() == "Dispatch" || (fn.Pkg() == e.es && strings.Contains(fn.Name(), "Detached")) {
				return true
			}
		}
	}
	if depth == 0 {
		return false
	}
	decl := e.decls[obj]
	dp := e.declPkg[obj]
	if decl == nil || decl.Body == nil || seen[decl] {
		return false
	}
	seen[decl] = true
	found := false
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		if found {
			return false
		}
		if inner, ok := node.(*ast.CallExpr); ok {
			if e.callDispatches(dp, inner, depth-1, seen) {
				found = true
			}
		}
		return true
	})
	return found
}

// scanBody walks a method body, then the bodies of called module functions
// up to depth levels deep.
func (e *extractor) scanBody(m method, depth int, visit func(*packages.Package, ast.Node)) {
	if m.decl == nil || m.decl.Body == nil || m.pkg == nil {
		return
	}
	seen := map[*ast.FuncDecl]bool{}
	var walk func(decl *ast.FuncDecl, pkg *packages.Package, d int)
	walk = func(decl *ast.FuncDecl, pkg *packages.Package, d int) {
		if decl == nil || decl.Body == nil || seen[decl] {
			return
		}
		seen[decl] = true
		ast.Inspect(decl.Body, func(node ast.Node) bool {
			visit(pkg, node)
			if d == 0 {
				return true
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var obj types.Object
			switch f := ast.Unparen(call.Fun).(type) {
			case *ast.Ident:
				obj = pkg.TypesInfo.Uses[f]
			case *ast.SelectorExpr:
				obj = pkg.TypesInfo.Uses[f.Sel]
			}
			if next := e.decls[obj]; next != nil {
				walk(next, e.declPkg[obj], d-1)
			}
			return true
		})
	}
	walk(m.decl, m.pkg, depth)
}

func sortedKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
