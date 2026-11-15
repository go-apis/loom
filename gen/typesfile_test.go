package gen_test

import (
	"bytes"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// two schema files: a.loom declares an enum, a value type, a read model
// and an event two aggregates emit; b.loom declares an enum and a value
// type of its own and uses a.loom's
var typesFileSchema = map[string]string{
	"schema/a.loom": `
service shop

enum E { one two }

type V { line: string! }

event Shared { note: string }

entity CartView {
  items: int
}

aggregate Cart {
  state {
    items: int
    e: E
  }
  command AddItem { items: int! } -> ItemAdded
  command Touch -> Shared
  event ItemAdded { items: int! }
}

projection carts -> CartView {
  on ItemAdded
}
`,
	"schema/b.loom": `
enum F { red green }

type W { v: V }

aggregate Shipment {
  state {
    f: F
    w: W
  }
  command Send { w: W! } -> ShipmentSent
  command Note -> Shared
  event ShipmentSent { sent: bool }
}
`,
}

// renderTypes renders the schema, with edit applied to each file's
// source first, and answers each generated file by its base name.
func renderTypes(t *testing.T, sources map[string]string, edit func(path, src string) string) map[string]string {
	t.Helper()
	var paths []string
	for path := range sources {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var files []sdl.File
	for _, path := range paths {
		src := sources[path]
		if edit != nil {
			src = edit(path, src)
		}
		files = append(files, sdl.File{Path: path, Src: src})
	}
	s, err := sdl.ParseFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := gen.Render(s, gen.Config{Dir: "/svc", Package: "shop", Module: "example.com/shop"})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for path, content := range rendered {
		out[filepath.Base(path)] = string(content)
	}
	return out
}

// TestTypesAreWrittenToTheirSchemaFilesFile: each schema file's enums,
// value types, read models and shared events are generated into
// <schema file>_types_gen.go; models_gen.go keeps none of them; an event
// only one aggregate emits stays in that aggregate's file; and the
// package builds.
func TestTypesAreWrittenToTheirSchemaFilesFile(t *testing.T) {
	files := renderTypes(t, typesFileSchema, nil)
	var names []string
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	if got, want := strings.Join(names, " "), "a_types_gen.go b_types_gen.go cart_gen.go models_gen.go registry_gen.go shipment_gen.go"; got != want {
		t.Fatalf("generated %s, want %s", got, want)
	}
	holds := map[string][]string{
		"a_types_gen.go":  {"type E string", "type V struct", "type Shared struct", "func (*Shared) LoomEvent()", "type CartView struct", "func (s *CartView) Fold("},
		"b_types_gen.go":  {"type F string", "type W struct"},
		"cart_gen.go":     {"type ItemAdded struct"},
		"shipment_gen.go": {"type ShipmentSent struct"},
	}
	for name, decls := range holds {
		for _, decl := range decls {
			if !strings.Contains(files[name], decl) {
				t.Errorf("%s lacks %q", name, decl)
			}
			for other, content := range files {
				if other != name && strings.Contains(content, decl) {
					t.Errorf("%q is in %s as well as %s", decl, other, name)
				}
			}
		}
	}
	if strings.Contains(files["models_gen.go"], "\ntype ") || strings.Contains(files["models_gen.go"], "\nfunc ") {
		t.Errorf("models_gen.go still declares something:\n%s", files["models_gen.go"])
	}

	// written to disk, the package compiles
	dir := t.TempDir()
	var in []sdl.File
	for _, path := range []string{"schema/a.loom", "schema/b.loom"} {
		in = append(in, sdl.File{Path: path, Src: typesFileSchema[path]})
	}
	s, err := sdl.ParseFiles(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gen.Generate(s, gen.Config{Dir: dir, Package: "shop", Module: "example.com/shop"}); err != nil {
		t.Fatal(err)
	}
	vetGenerated(t, dir, "example.com/shop")
	if stale, missing, err := gen.Check(s, gen.Config{Dir: dir, Package: "shop", Module: "example.com/shop"}); err != nil || len(stale)+len(missing) > 0 {
		t.Errorf("check after generate: stale %v, missing %v, %v", stale, missing, err)
	}
}

// TestTypesFilesAreTheSameOnEveryRun: generating twice gives the same
// bytes in the same files.
func TestTypesFilesAreTheSameOnEveryRun(t *testing.T) {
	first := renderTypes(t, typesFileSchema, nil)
	for i := 0; i < 20; i++ {
		again := renderTypes(t, typesFileSchema, nil)
		if len(again) != len(first) {
			t.Fatalf("run %d wrote %d files, the first %d", i, len(again), len(first))
		}
		for name, want := range first {
			if again[name] != want {
				t.Fatalf("run %d: %s differs", i, name)
			}
		}
	}
}

// TestEditsToDifferentSchemaFilesTouchNoGeneratedFileInCommon: one
// branch adds a value to an enum in a.loom, another adds a field to a
// value type in b.loom. Regenerated, the two change different files.
func TestEditsToDifferentSchemaFilesTouchNoGeneratedFileInCommon(t *testing.T) {
	base := renderTypes(t, typesFileSchema, nil)
	changed := func(files map[string]string) []string {
		var out []string
		for name, content := range files {
			if base[name] != content {
				out = append(out, name)
			}
		}
		for name := range base {
			if _, ok := files[name]; !ok {
				out = append(out, name)
			}
		}
		sort.Strings(out)
		return out
	}
	enumBranch := changed(renderTypes(t, typesFileSchema, func(path, src string) string {
		if path != "schema/a.loom" {
			return src
		}
		return strings.Replace(src, "enum E { one two }", "enum E { one two three }", 1)
	}))
	typeBranch := changed(renderTypes(t, typesFileSchema, func(path, src string) string {
		if path != "schema/b.loom" {
			return src
		}
		return strings.Replace(src, "type W { v: V }", "type W {\n  v: V\n  weight: int\n}", 1)
	}))
	if got := strings.Join(enumBranch, " "); got != "a_types_gen.go registry_gen.go" && got != "a_types_gen.go" {
		t.Errorf("adding an enum value in a.loom changed %s", got)
	}
	if got := strings.Join(typeBranch, " "); got != "b_types_gen.go" {
		t.Errorf("adding a field in b.loom changed %s", got)
	}
	for _, a := range enumBranch {
		for _, b := range typeBranch {
			if a == b {
				t.Errorf("both branches changed %s", a)
			}
		}
	}
}

// TestTypesFileNamesNeverCollide: two schema files of one name in
// different directories each get a file; a schema file whose name a Go
// file cannot carry is renamed; an aggregate whose own file would be a
// schema file's types file moves aside; and a lone unnamed source, which
// has no file to be named after, keeps everything in models_gen.go.
func TestTypesFileNamesNeverCollide(t *testing.T) {
	files := renderTypes(t, map[string]string{
		"schema/a/goal.loom":    "service shop\n\nenum One { a }\n",
		"schema/b/goal.loom":    "service shop\n\nenum Two { b }\n",
		"schema/_my-types.loom": "service shop\n\nenum Three { c }\n",
		"schema/z.loom":         "service shop\n\naggregate GoalTypes {\n  state { n: int }\n  command Bump -> Bumped\n  event Bumped { n: int }\n}\n",
	}, nil)
	for name, decl := range map[string]string{
		"goal_types_gen.go":           "type One string",
		"goal_2_types_gen.go":         "type Two string",
		"my_types_types_gen.go":       "type Three string",
		"goal_types_aggregate_gen.go": "type GoalTypes struct",
	} {
		if !strings.Contains(files[name], decl) {
			var names []string
			for n := range files {
				names = append(names, n)
			}
			sort.Strings(names)
			t.Errorf("%s lacks %q; generated %v", name, decl, names)
		}
	}

	s, err := sdl.Parse("service shop\n\nenum One { a }\n\ntype V { line: string }\n")
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := gen.Render(s, gen.Config{Dir: "/svc", Package: "shop", Module: "example.com/shop"})
	if err != nil {
		t.Fatal(err)
	}
	for path, content := range rendered {
		name := filepath.Base(path)
		if strings.HasSuffix(name, "_types_gen.go") {
			t.Errorf("an unnamed source generated %s", name)
		}
		if name == "models_gen.go" && !(bytes.Contains(content, []byte("type One string")) && bytes.Contains(content, []byte("type V struct"))) {
			t.Errorf("models_gen.go lacks the unnamed source's types:\n%s", content)
		}
	}
}
