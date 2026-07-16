// Package extract derives a Loom schema from an existing service's source,
// mirroring the runtime reflection rules of es.NewRegistry (registry.go and
// the *handle.go finders) as static analysis. It never executes service code.
package extract

import (
	"fmt"
	"go/ast"
	"go/types"
	"sort"

	"github.com/go-apis/loom/schema"
	"golang.org/x/tools/go/packages"
)

const esPath = "github.com/go-apis/eventsourcing/es"

type Result struct {
	Schema   *schema.Schema
	Warnings []string
}

type extractor struct {
	service string
	pkgs    []*packages.Package

	es     *types.Package
	ifaces map[string]*types.Interface // IsSaga, IsProjector, ... plus Aggregate, Entity, Command
	ctx    types.Type                  // context.Context
	event  types.Type                  // es.Event

	decls   map[types.Object]*ast.FuncDecl
	declPkg map[types.Object]*packages.Package

	out      *schema.Schema
	events   map[string]*schema.Event
	types    map[string]*schema.NamedType
	warnings []string
}

// Service extracts the Loom schema for the module rooted at dir, registered
// under the given service name (the runtime name comes from ProviderConfig,
// which is not statically knowable).
func Service(dir, service string) (*Result, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedImports | packages.NeedDeps | packages.NeedTypes |
			packages.NeedSyntax | packages.NeedTypesInfo | packages.NeedModule,
		Dir: dir,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, err
	}

	e := &extractor{
		service: service,
		pkgs:    pkgs,
		ifaces:  map[string]*types.Interface{},
		decls:   map[types.Object]*ast.FuncDecl{},
		declPkg: map[types.Object]*packages.Package{},
		out:     &schema.Schema{Loom: schema.Version, Service: service},
		events:  map[string]*schema.Event{},
		types:   map[string]*schema.NamedType{},
	}

	for _, p := range pkgs {
		for _, perr := range p.Errors {
			e.warnf("load: %v", perr)
		}
	}

	e.indexDecls()
	if err := e.resolveEs(); err != nil {
		return nil, err
	}

	calls := e.findRegistryCalls()
	if len(calls) == 0 {
		return nil, fmt.Errorf("%s: no es.NewRegistry call found", dir)
	}
	if len(calls) > 1 {
		e.warnf("%d es.NewRegistry calls found; extracting the union", len(calls))
	}
	for _, c := range calls {
		e.registry(c.pkg, c.call)
	}

	for _, ev := range e.events {
		e.out.Events = append(e.out.Events, ev)
	}
	for _, t := range e.types {
		e.out.Types = append(e.out.Types, t)
	}
	e.out.Sort()
	sort.Strings(e.warnings)
	return &Result{Schema: e.out, Warnings: e.warnings}, nil
}

func (e *extractor) warnf(format string, args ...any) {
	e.warnings = append(e.warnings, fmt.Sprintf(format, args...))
}

// indexDecls maps every function/method object defined in the loaded module
// to its AST declaration, so handler bodies can be analyzed.
func (e *extractor) indexDecls() {
	for _, p := range e.pkgs {
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok {
					continue
				}
				obj := p.TypesInfo.Defs[fd.Name]
				if obj == nil {
					continue
				}
				e.decls[obj] = fd
				e.declPkg[obj] = p
			}
		}
	}
}

func (e *extractor) resolveEs() error {
	packages.Visit(e.pkgs, nil, func(p *packages.Package) {
		switch p.PkgPath {
		case esPath:
			e.es = p.Types
		case "context":
			if obj := p.Types.Scope().Lookup("Context"); obj != nil {
				e.ctx = obj.Type()
			}
		}
	})
	if e.es == nil {
		return fmt.Errorf("module does not depend on %s", esPath)
	}
	for _, name := range []string{"IsSaga", "IsProjector", "IsEventHandler", "IsCommandHandler", "IsEvent", "Aggregate", "AggregateHolder", "Entity", "Command"} {
		obj := e.es.Scope().Lookup(name)
		if obj == nil {
			return fmt.Errorf("es.%s not found", name)
		}
		iface, ok := obj.Type().Underlying().(*types.Interface)
		if !ok {
			return fmt.Errorf("es.%s is not an interface", name)
		}
		e.ifaces[name] = iface
	}
	if obj := e.es.Scope().Lookup("Event"); obj != nil {
		e.event = obj.Type()
	}
	if e.ctx == nil || e.event == nil {
		return fmt.Errorf("context.Context or es.Event not resolved")
	}
	return nil
}

type registryCall struct {
	pkg  *packages.Package
	call *ast.CallExpr
}

func (e *extractor) findRegistryCalls() []registryCall {
	var calls []registryCall
	for _, p := range e.pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				fn, ok := p.TypesInfo.Uses[sel.Sel].(*types.Func)
				if !ok || fn.FullName() != esPath+".NewRegistry" {
					return true
				}
				calls = append(calls, registryCall{pkg: p, call: call})
				return true
			})
		}
	}
	return calls
}

// registry processes one es.NewRegistry call, classifying each registered
// item with the same precedence as the runtime type-switch.
func (e *extractor) registry(p *packages.Package, call *ast.CallExpr) {
	if len(call.Args) < 2 {
		return
	}
	for _, arg := range call.Args[1:] {
		named := e.resolveConcrete(p, arg, 4)
		if named == nil {
			e.warnf("cannot resolve registry item to a concrete type: %s", exprString(p, arg))
			continue
		}
		switch e.classify(named) {
		case "saga":
			e.addSubscriber(named, "saga")
		case "projector":
			e.addProjector(named)
		case "event_handler":
			e.addSubscriber(named, "event_handler")
		case "command_handler":
			e.addCommandHandler(named)
		case "event":
			e.registerEvent(named)
		case "aggregate":
			e.addAggregate(named, true)
		case "entity":
			e.addAggregate(named, false)
		default:
			e.warnf("registry item %s matches no registrable interface", named.Obj().Name())
		}
	}
}

// resolveConcrete resolves a registry argument to the concrete named struct
// behind it: a (possibly &-prefixed) composite literal directly, or the type
// returned by a constructor, chasing return statements through module code.
func (e *extractor) resolveConcrete(p *packages.Package, expr ast.Expr, depth int) *types.Named {
	if n := concreteNamed(p.TypesInfo.TypeOf(expr)); n != nil {
		return n
	}
	if depth == 0 {
		return nil
	}
	call, ok := ast.Unparen(expr).(*ast.CallExpr)
	if !ok {
		return nil
	}
	var obj types.Object
	switch f := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		obj = p.TypesInfo.Uses[f]
	case *ast.SelectorExpr:
		obj = p.TypesInfo.Uses[f.Sel]
	}
	decl := e.decls[obj]
	dp := e.declPkg[obj]
	if decl == nil || decl.Body == nil {
		return nil
	}
	var found *types.Named
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		if found != nil {
			return false
		}
		ret, ok := node.(*ast.ReturnStmt)
		if !ok || len(ret.Results) == 0 {
			return true
		}
		if n := e.resolveConcrete(dp, ret.Results[0], depth-1); n != nil {
			found = n
		}
		return true
	})
	return found
}

// concreteNamed unwraps pointers and returns the named struct type, or nil
// for interfaces and anything unnamed.
func concreteNamed(t types.Type) *types.Named {
	if t == nil {
		return nil
	}
	if ptr, ok := t.Underlying().(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok {
		return nil
	}
	if _, ok := named.Underlying().(*types.Struct); !ok {
		return nil
	}
	return named
}

// classify mirrors the registry type-switch, in its exact order.
func (e *extractor) classify(n *types.Named) string {
	ptr := types.NewPointer(n)
	for _, c := range []struct{ iface, kind string }{
		{"IsSaga", "saga"},
		{"IsProjector", "projector"},
		{"IsEventHandler", "event_handler"},
		{"IsCommandHandler", "command_handler"},
		{"IsEvent", "event"},
		{"Aggregate", "aggregate"},
		{"Entity", "entity"},
	} {
		if types.Implements(ptr, e.ifaces[c.iface]) {
			return c.kind
		}
	}
	return ""
}

func goPath(n *types.Named) string {
	return n.Obj().Pkg().Path() + "." + n.Obj().Name()
}

func exprString(p *packages.Package, expr ast.Expr) string {
	if t := p.TypesInfo.TypeOf(expr); t != nil {
		return t.String()
	}
	return fmt.Sprintf("%T", expr)
}
