package gen

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-apis/loom/schema"
)

// Wiring by convention (loom.yml `wire:`, with `layout: folders`).
//
// A registry needs an implementation for every aggregate, record,
// policy, process and @fold projection the schema declares. Under the
// folders layout each has an obvious home — aggregates.Order,
// processes.ShipOnPayment — and the only thing a hand-kept registry.go
// adds is a line per declaration saying so: one shared list that every
// new declaration edits, and that two branches adding one each collide
// on.
//
// With `wire:` set, loom writes that line itself, into a file of its
// own per declaration, in a generated package (wire/ by default):
//
//	wire/order_aggregate_gen.go          impl.Order(&aggregates.Order{})
//	wire/shiponpayment_process_gen.go    impl.ShipOnPayment = processes.NewShipOnPayment(deps)
//	wire/wire_gen.go                     Impl(deps), Configure — never changes with the schema
//
// The implementation of a declaration X is, in its kind's package, the
// constructor NewX when there is one — taking nothing, or the one
// dependency type every constructor of the service shares — and
// otherwise the type X. loom reads the packages to tell which, so a
// declaration with neither yet is wired to the type its stub is about
// to give it.
//
// What a convention cannot say stays hand-written beside the generated
// files: wire.Configure(func(*aggregates.Goal, Deps)) sets a handler up
// before it is bound, and the service's own registry.go builds the
// registry from wire.Impl(deps), where it can still set Upcasts and
// touch the result.

// impl is how one declaration's implementation is built.
type impl struct {
	// expr is the Go expression: "&aggregates.Order{}",
	// "processes.NewShipOnPayment(deps)".
	expr string
}

// kindScan is what one kind's package declares that wiring cares about.
type kindScan struct {
	types map[string]bool
	// ctors maps a constructor's name to its one parameter's type as
	// the wire package writes it ("*processes.Services"), "" for none.
	ctors map[string]string
	// imports maps a parameter type to the import it needs beyond the
	// kind's own package, "" for none.
	imports map[string]string
}

// scanKind reads a kind's package for types and New* constructors. A
// directory that does not exist declares nothing.
func (g *generator) scanKind(kind string) (*kindScan, error) {
	out := &kindScan{types: map[string]bool{}, ctors: map[string]string{}, imports: map[string]string{}}
	dir := filepath.Join(g.cfg.Dir, kind)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("wire: %w", err)
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.IsExported() {
						out.types[ts.Name.Name] = true
					}
				}
			case *ast.FuncDecl:
				if d.Recv != nil || !strings.HasPrefix(d.Name.Name, "New") || !d.Name.IsExported() {
					continue
				}
				params := d.Type.Params.NumFields()
				if params > 1 || d.Type.Results.NumFields() != 1 {
					// not a constructor the convention can call; a type
					// of the declaration's name may still be there
					continue
				}
				if params == 0 {
					out.ctors[d.Name.Name] = ""
					continue
				}
				typ, imp, err := g.paramType(fset, file, kind, d.Type.Params.List[0].Type)
				if err != nil {
					return nil, fmt.Errorf("wire: %s.%s: %w", kind, d.Name.Name, err)
				}
				out.ctors[d.Name.Name] = typ
				out.imports[typ] = imp
			}
		}
	}
	return out, nil
}

// paramType renders a constructor's parameter type as another package
// writes it: a type of the kind's own package is qualified with it, and
// one of another package brings that package's import.
func (g *generator) paramType(fset *token.FileSet, file *ast.File, kind string, expr ast.Expr) (typ, imp string, err error) {
	star := ""
	if p, ok := expr.(*ast.StarExpr); ok {
		star, expr = "*", p.X
	}
	switch t := expr.(type) {
	case *ast.Ident:
		if !t.IsExported() {
			return "", "", fmt.Errorf("its parameter is of an unexported type %s, which nothing outside %s can pass", t.Name, kind)
		}
		return star + kind + "." + t.Name, "", nil
	case *ast.SelectorExpr:
		pkg, ok := t.X.(*ast.Ident)
		if !ok {
			break
		}
		for _, spec := range file.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			name := filepath.Base(path)
			if spec.Name != nil {
				name = spec.Name.Name
			}
			if name == pkg.Name {
				return star + filepath.Base(path) + "." + t.Sel.Name, path, nil
			}
		}
	}
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, expr)
	return "", "", fmt.Errorf("its parameter type %s is not one the convention can pass: take a named type, or a pointer to one", b.String())
}

// wireEntry is one declaration's wiring file.
type wireEntry struct {
	file string // "order_aggregate_gen.go"
	kind string // the kind's package
	name string // the implementation's name: Order, ShipOnPayment
	// bind writes the statement that hands h to the Impl.
	bind string
}

func (g *generator) wireEntries() []wireEntry {
	var out []wireEntry
	for _, a := range g.s.Aggregates {
		out = append(out, wireEntry{strings.ToLower(a.Name) + "_aggregate_gen.go", kindAggregates, a.Name, "impl." + a.Name + "(h)"})
	}
	for _, r := range g.s.Records {
		out = append(out, wireEntry{strings.ToLower(r.Name) + "_record_gen.go", kindRecords, r.Name, "impl." + r.Name + "(h)"})
	}
	reactor := func(kind, suffix string, r *schema.Reactor) {
		out = append(out, wireEntry{strings.ToLower(r.Name) + "_" + suffix + "_gen.go", kind, exported(r.Name), "impl." + g.implField(r.Name) + " = h"})
	}
	for _, r := range g.s.Policies {
		reactor(kindPolicies, "policy", r)
	}
	for _, r := range g.s.Processes {
		reactor(kindProcesses, "process", r)
	}
	for _, p := range g.s.Projections {
		if p.Fold {
			out = append(out, wireEntry{strings.ToLower(p.Name) + "_projection_gen.go", kindProjections, exported(p.Name), "impl." + g.implField(p.Name) + " = h"})
		}
	}
	return out
}

// wireFiles renders the wire package: one file per declaration and the
// fixed wire_gen.go. Empty when wiring is off.
func (g *generator) wireFiles() (map[string]string, error) {
	if g.cfg.Wire == "" {
		return nil, nil
	}
	if !g.folders() {
		return nil, fmt.Errorf("loom.yml: wire needs layout: folders — the convention is a declaration's kind package")
	}
	dir := filepath.Join(g.cfg.Dir, g.cfg.Wire)
	pkg := identLower(filepath.Base(g.cfg.Wire))
	genImport := g.cfg.Module + "/" + filepath.ToSlash(g.cfg.GenDir)

	scans := map[string]*kindScan{}
	for _, kind := range []string{kindAggregates, kindRecords, kindPolicies, kindProcesses, kindProjections} {
		scan, err := g.scanKind(kind)
		if err != nil {
			return nil, err
		}
		scans[kind] = scan
	}
	entries := g.wireEntries()

	// the one dependency type every constructor shares, if any takes one
	deps, depsImport, depsFrom := "", "", ""
	for _, e := range entries {
		typ, ok := scans[e.kind].ctors["New"+e.name]
		if !ok || typ == "" {
			continue
		}
		if deps != "" && typ != deps {
			return nil, fmt.Errorf("wire: %s.New%s takes %s and %s takes %s: constructors wired by convention share one dependency type", e.kind, e.name, typ, depsFrom, deps)
		}
		deps, depsImport, depsFrom = typ, scans[e.kind].imports[typ], e.kind+".New"+e.name
	}

	files := map[string]string{}
	for _, e := range entries {
		expr := "&" + e.kind + "." + e.name + "{}"
		if typ, ok := scans[e.kind].ctors["New"+e.name]; ok {
			expr = e.kind + ".New" + e.name + "()"
			if typ != "" {
				expr = e.kind + ".New" + e.name + "(deps)"
			}
		}
		var b strings.Builder
		b.WriteString(header)
		fmt.Fprintf(&b, "package %s\n\nimport (\n\t%q\n\t%q\n)\n\n", pkg, genImport, g.cfg.Module+"/"+e.kind)
		fmt.Fprintf(&b, "func init() {\n\tadd(func(impl *%s.Impl, deps Deps) {\n\t\th := %s\n\t\tconfigure(h, deps)\n\t\t%s\n\t})\n}\n", g.genPkg, expr, e.bind)
		files[filepath.Join(dir, e.file)] = b.String()
	}

	var b strings.Builder
	b.WriteString(header)
	fmt.Fprintf(&b, "// Package %s wires the service's implementations into its generated\n// registry by convention: one generated file per declaration, each\n// adding itself, so a new aggregate or process edits no shared list.\n// This file is the fixed part.\n", pkg)
	fmt.Fprintf(&b, "package %s\n\nimport (\n\t%q\n", pkg, genImport)
	depsType := "struct{}"
	if deps != "" {
		depsType = deps
		imp := depsImport
		if imp == "" {
			imp = g.cfg.Module + "/" + strings.SplitN(strings.TrimPrefix(deps, "*"), ".", 2)[0]
		}
		fmt.Fprintf(&b, "\t%q\n", imp)
	}
	b.WriteString(")\n\n")
	fmt.Fprintf(&b, "// Deps is what the service's constructors are given: the one dependency\n// type they share, or nothing when none takes any.\ntype Deps = %s\n\n", depsType)
	fmt.Fprintf(&b, `var (
	entries []func(impl *%[1]s.Impl, deps Deps)
	setups  []func(h any, deps Deps)
)

// add is called by each declaration's generated file.
func add(e func(impl *%[1]s.Impl, deps Deps)) { entries = append(entries, e) }

// Configure sets up every implementation of type T before it is bound —
// the wiring no convention can say: a callback only another package can
// supply, a switch read from deps. Call it from an init in a hand-written
// file of this package. T is the implementation's pointer type
// (*aggregates.Goal), or an interface several implement.
func Configure[T any](f func(h T, deps Deps)) {
	setups = append(setups, func(h any, deps Deps) {
		if t, ok := h.(T); ok {
			f(t, deps)
		}
	})
}

func configure(h any, deps Deps) {
	for _, s := range setups {
		s(h, deps)
	}
}

// Impl builds the generated Impl with every implementation the schema
// declares, each constructed by convention and set up by Configure.
// Upcasts and Look, which no kind package holds, are left for the
// caller to set.
func Impl(deps Deps) %[1]s.Impl {
	impl := %[1]s.Impl{}
	for _, e := range entries {
		e(&impl, deps)
	}
	return impl
}
`, g.genPkg)
	files[filepath.Join(dir, "wire_gen.go")] = b.String()

	return files, nil
}
