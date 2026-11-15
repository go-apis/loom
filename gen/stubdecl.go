package gen

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// declaredElsewhere is the file of stub's directory that already
// declares one of the stub's top-level types or functions, or "". A
// stub is named after its declaration (order.go for Order), but the
// implementation is the owner's to name: one that lives in icp.go for
// IdealCustomerProfile is the same implementation, and a second file
// declaring the type would not compile. Test files and generated files
// are not the owner's implementation and are not read; a file that
// does not parse is passed over, as the compiler will name it.
func declaredElsewhere(stubPath, content string) string {
	want := topLevelNames(content)
	if len(want) == 0 {
		return ""
	}
	dir := filepath.Dir(stubPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") || strings.HasSuffix(n, "_gen.go") {
			continue
		}
		path := filepath.Join(dir, n)
		if path == stubPath {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for name := range topLevelNames(string(src)) {
			if want[name] {
				return path
			}
		}
	}
	return ""
}

// topLevelNames is the types and plain functions (not methods) a Go
// source declares at top level.
func topLevelNames(src string) map[string]bool {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				out[d.Name.Name] = true
			}
		case *ast.GenDecl:
			if d.Tok != token.TYPE {
				continue
			}
			for _, s := range d.Specs {
				out[s.(*ast.TypeSpec).Name.Name] = true
			}
		}
	}
	return out
}
