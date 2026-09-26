package sdl_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-apis/loom/sdl"
)

// writeTree writes path→source files under a fresh temp dir.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, src := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// The event OrderPlaced is declared in b.loom; the aggregate in a.loom
// produces it. One parse over both files resolves the reference.
var crossFile = map[string]string{
	"a.loom": `service orders

aggregate Order {
  state {
    status: string
  }
  command PlaceOrder {
    status: string!
  } -> OrderPlaced
}
`,
	"b.loom": `service orders

event OrderPlaced {
  status: string!
}
`,
}

func TestParseDirCrossFileReference(t *testing.T) {
	s, err := sdl.ParseDir(writeTree(t, crossFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Aggregates) != 1 || len(s.Events) != 1 || s.Events[0].Name != "OrderPlaced" {
		t.Fatalf("cross-file schema misparsed: aggregates %d, events %+v", len(s.Aggregates), s.Events)
	}
}

func TestParseDirPathOrder(t *testing.T) {
	files := map[string]string{
		"a.loom":             "service orders\n",
		"billing/z.loom":     "enum Currency { aud usd }\n",
		"billing/sub/y.loom": "type Money {\n  cents: int!\n}\n",
		"orders/order.loom":  crossFile["a.loom"],
		"orders/events.loom": crossFile["b.loom"],
		"notes.txt":          "not a schema",
	}
	got, err := sdl.ParseDir(writeTree(t, files))
	if err != nil {
		t.Fatal(err)
	}
	// The same files in path order, given explicitly.
	order := []string{"a.loom", "billing/sub/y.loom", "billing/z.loom", "orders/events.loom", "orders/order.loom"}
	var in []sdl.File
	for _, rel := range order {
		in = append(in, sdl.File{Path: rel, Src: files[rel]})
	}
	want, err := sdl.ParseFiles(in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseDir differs from ParseFiles in path order")
	}

	// A file system that lists every directory backwards gives the same
	// schema: the order is the paths', not the listing's.
	mapfs := fstest.MapFS{}
	for rel, src := range files {
		mapfs[rel] = &fstest.MapFile{Data: []byte(src)}
	}
	fwd, err := sdl.ParseFS(mapfs)
	if err != nil {
		t.Fatal(err)
	}
	back, err := sdl.ParseFS(reversedFS{mapfs})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fwd, want) || !reflect.DeepEqual(back, want) {
		t.Fatalf("ParseFS depends on listing order")
	}
}

// reversedFS lists each directory in reverse name order.
type reversedFS struct{ fstest.MapFS }

func (r reversedFS) ReadDir(name string) ([]fs.DirEntry, error) {
	es, err := r.MapFS.ReadDir(name)
	slices.Reverse(es)
	return es, err
}

func TestParseFilesErrorNamesFileAndLine(t *testing.T) {
	_, err := sdl.ParseFiles([]sdl.File{
		{Path: "schema/a.loom", Src: crossFile["a.loom"]},
		{Path: "schema/b.loom", Src: "service orders\n\nevent OrderPlaced {\n  status string!\n}\n"},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "schema/b.loom:4:") {
		t.Fatalf("want an error at schema/b.loom:4:, got %v", err)
	}
	_, err = sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "service orders\n"},
		{Path: "b.loom", Src: "\n\n  $"},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "b.loom:3:") {
		t.Fatalf("want a lex error at b.loom:3:, got %v", err)
	}
}

func TestParseFilesServiceHeader(t *testing.T) {
	_, err := sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "service orders\n"},
		{Path: "b.loom", Src: "// billing\nservice billing\n"},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "b.loom:2:") || !strings.Contains(err.Error(), "service billing") {
		t.Fatalf("want a mismatched service refused at b.loom:2:, got %v", err)
	}
	// the first file must open with the header
	_, err = sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "type T {\n  x: int\n}\n"},
		{Path: "b.loom", Src: "service orders\n"},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "a.loom:1:") {
		t.Fatalf("want a missing header refused at a.loom:1:, got %v", err)
	}
	_, err = sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "// nothing yet\n"},
		{Path: "b.loom", Src: "service orders\n"},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "a.loom:") {
		t.Fatalf("want an empty first file refused at a.loom, got %v", err)
	}
	// a header is only a header at the top of a file
	_, err = sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "service orders\ntype T {\n  x: int\n}\nservice orders\n"},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "a.loom:5:") {
		t.Fatalf("want a mid-file header refused at a.loom:5:, got %v", err)
	}
	// later files may omit the header or repeat it; empty files are fine
	s, err := sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "service orders\n"},
		{Path: "b.loom", Src: "service orders\ntype B {\n  x: int\n}\n"},
		{Path: "c.loom", Src: ""},
		{Path: "d.loom", Src: "type D {\n  x: int\n}\n"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.Service != "orders" || len(s.Types) != 2 {
		t.Fatalf("service %q, types %d", s.Service, len(s.Types))
	}
}

func TestParseFilesEnumsAndSeriesFromLaterFile(t *testing.T) {
	s, err := sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "service metrics\n"},
		{Path: "b.loom", Src: `enum Route { home about }

series Hit @time(at) @dim(route) {
  route: Route!
  at:    timestamp!
}
`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Enums) != 1 || s.Enums[0].Name != "Route" {
		t.Fatalf("enums = %+v", s.Enums)
	}
	if len(s.Series) != 1 || s.Series[0].Name != "Hit" {
		t.Fatalf("series = %+v", s.Series)
	}
}

func TestParseIsParseFilesOverOneFile(t *testing.T) {
	one, err := sdl.Parse(valid)
	if err != nil {
		t.Fatal(err)
	}
	many, err := sdl.ParseFiles([]sdl.File{{Path: "orders.loom", Src: valid}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(one, many) {
		t.Fatal("Parse and ParseFiles over the same file differ")
	}
	// only the error text differs: an unnamed source says "line N:"
	_, err = sdl.Parse("service orders\n\n}")
	if err == nil || !strings.HasPrefix(err.Error(), "line 3:") {
		t.Fatalf("want line 3:, got %v", err)
	}
}
