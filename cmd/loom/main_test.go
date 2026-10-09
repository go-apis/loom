package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestLoadSchemasDirFileGlob(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, src string) {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// the aggregate in orders/order.loom emits the event from events.loom
	write("schema/events.loom", "service orders\n\nevent OrderPlaced {\n  status: string!\n}\n")
	write("schema/orders/order.loom", `aggregate Order {
  state {
    status: string
  }
  command PlaceOrder {
    status: string!
  } -> OrderPlaced
}
`)
	for _, spec := range []string{"schema", "schema/", "schema/*.loom"} {
		s, err := loadSchemas(dir, spec)
		if spec == "schema/*.loom" {
			// the glob misses orders/, so the event has no producer — but it parses
			if err != nil || len(s.Aggregates) != 0 || len(s.Events) != 1 {
				t.Fatalf("%s: %v", spec, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		if s.Service != "orders" || len(s.Aggregates) != 1 || len(s.Events) != 1 {
			t.Fatalf("%s: service %q, %d aggregates, %d events", spec, s.Service, len(s.Aggregates), len(s.Events))
		}
	}
	if _, err := loadSchemas(dir, "schema/events.loom"); err != nil {
		t.Fatalf("single file: %v", err)
	}
	if err := runCheck([]string{filepath.Join(dir, "schema")}); err != nil {
		t.Fatalf("check dir: %v", err)
	}
}

// The README's example layout, exactly: billing/invoice.loom sorts before
// orders.loom, so it is the first file and carries the header like the rest.
func TestReadmeLayoutChecks(t *testing.T) {
	files := map[string]string{
		"billing/invoice.loom": `service orders

aggregate Invoice {
  state {
    status: InvoiceStatus
  }
  command IssueInvoice {
    order_id: string!
  } -> InvoiceIssued
}
`,
		"orders.loom": `service orders

enum InvoiceStatus { draft issued }

event InvoiceIssued {
  order_id: string!
}

event ShipmentSent {
  order_id: string!
}
`,
		"shipping/shipment.loom": `service orders

aggregate Shipment {
  state {
    order_id: string
  }
  command SendShipment {
    order_id: string!
  } -> ShipmentSent
}
`,
	}
	dir := t.TempDir()
	write := func(rel, src string) {
		p := filepath.Join(dir, "schema", filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for rel, src := range files {
		write(rel, src)
	}
	s, err := loadSchemas(dir, "schema/")
	if err != nil {
		t.Fatal(err)
	}
	if s.Service != "orders" || len(s.Aggregates) != 2 || len(s.Events) != 2 || len(s.Enums) != 1 {
		t.Fatalf("service %q, %d aggregates, %d events, %d enums", s.Service, len(s.Aggregates), len(s.Events), len(s.Enums))
	}
	if err := runCheck([]string{filepath.Join(dir, "schema")}); err != nil {
		t.Fatalf("check dir: %v", err)
	}

	// Why the README says to repeat the header: without it the first file
	// by path is headerless and the schema is refused.
	write("billing/invoice.loom", strings.TrimPrefix(files["billing/invoice.loom"], "service orders\n"))
	_, err = loadSchemas(dir, "schema/")
	if err == nil || !strings.Contains(err.Error(), filepath.Join("schema", "billing", "invoice.loom")+":") {
		t.Fatalf("want a refusal naming billing/invoice.loom, got %v", err)
	}
}

const checkSchema = `service orders

event OrderPlaced {
  status: string!
}

aggregate Order {
  state {
    status: string
  }
  command PlaceOrder {
    status: string!
  } -> OrderPlaced
  command CancelOrder {
    reason: string
  } -> OrderPlaced
}
`

// checkFixture generates a service in a temp dir and returns helpers.
func checkFixture(t *testing.T) (dir string, write func(string, string), read func(string) string, check func() (string, error)) {
	t.Helper()
	dir = t.TempDir()
	write = func(rel, src string) {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	read = func(rel string) string {
		b, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	check = func() (string, error) {
		var out bytes.Buffer
		err := runGenerate([]string{"--dir", dir, "--check"}, &out)
		return out.String(), err
	}
	write("go.mod", "module example.com/orders\n\ngo 1.22\n")
	write("loom.yml", "schema: schema\n")
	write("schema/orders.loom", checkSchema)
	if err := runGenerate([]string{"--dir", dir}, io.Discard); err != nil {
		t.Fatal(err)
	}
	return
}

func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		m[p] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestGenerateCheckClean(t *testing.T) {
	dir, _, _, check := checkFixture(t)
	before := snapshot(t, dir)
	out, err := check()
	if err != nil || out != "" {
		t.Fatalf("clean tree: err %v, out %q", err, out)
	}
	if !reflect.DeepEqual(before, snapshot(t, dir)) {
		t.Fatal("--check changed files")
	}
}

func TestGenerateCheckStaleRegistry(t *testing.T) {
	dir, write, read, check := checkFixture(t)
	// a policy adds a reactions interface to the registry only
	write("schema/orders.loom", checkSchema+"\npolicy autoCancel {\n  on OrderPlaced -> CancelOrder\n}\n")
	before := snapshot(t, dir)
	out, err := check()
	if err == nil || strings.TrimSpace(out) != "stale   loomgen/registry_gen.go" {
		t.Fatalf("schema edit: err %v, out %q", err, out)
	}
	if !reflect.DeepEqual(before, snapshot(t, dir)) {
		t.Fatal("--check changed files")
	}

	// hand edit, on a fresh tree
	write("schema/orders.loom", checkSchema)
	edited := read("loomgen/registry_gen.go") + "// hand edit\n"
	write("loomgen/registry_gen.go", edited)
	out, err = check()
	if err == nil || strings.TrimSpace(out) != "stale   loomgen/registry_gen.go" {
		t.Fatalf("hand edit: err %v, out %q", err, out)
	}
	if read("loomgen/registry_gen.go") != edited {
		t.Fatal("--check rewrote the hand-edited file")
	}
}

func TestGenerateCheckMissing(t *testing.T) {
	dir, _, _, check := checkFixture(t)
	p := filepath.Join(dir, "loomgen/models_gen.go")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	out, err := check()
	if err == nil || strings.TrimSpace(out) != "missing loomgen/models_gen.go" {
		t.Fatalf("err %v, out %q", err, out)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("--check recreated the file")
	}
}

func TestGenerateCheckIgnoresStubs(t *testing.T) {
	dir, write, read, check := checkFixture(t)
	var stubs []string
	for p := range snapshot(t, dir) {
		r, _ := filepath.Rel(dir, p)
		if strings.HasSuffix(r, ".go") && !strings.HasPrefix(r, "loomgen") {
			stubs = append(stubs, r)
		}
	}
	if len(stubs) < 2 {
		t.Fatalf("want at least 2 stubs, got %v", stubs)
	}
	sort.Strings(stubs)
	write(stubs[0], read(stubs[0])+"\n// mine\n")
	if err := os.Remove(filepath.Join(dir, stubs[1])); err != nil {
		t.Fatal(err)
	}
	out, err := check()
	if err != nil || out != "" {
		t.Fatalf("err %v, out %q", err, out)
	}
	if _, err := os.Stat(filepath.Join(dir, stubs[1])); !os.IsNotExist(err) {
		t.Fatal("--check recreated a stub")
	}
}

func initIn(t *testing.T, gomod string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	if gomod != "" {
		if err := os.WriteFile("go.mod", []byte(gomod), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := runInit([]string{"svc"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile("go.mod")
	return dir, string(b)
}

func TestInitPinsGeneratorOnce(t *testing.T) {
	_, got := initIn(t, "module example.com/svc\n\ngo 1.24\n")
	if n := strings.Count(got, "tool "+loomTool); n != 1 {
		t.Fatalf("want one tool line, got %d:\n%s", n, got)
	}
	if err := pinGenerator("go.mod", io.Discard); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile("go.mod")
	if string(again) != got {
		t.Fatalf("second application changed go.mod:\n%s", again)
	}
}

func TestInitKeepsExistingToolDirective(t *testing.T) {
	for name, gomod := range map[string]string{
		"single": "module x\n\ngo 1.24\n\ntool github.com/go-apis/loom/cmd/loom\n",
		"block":  "module x\n\ngo 1.24\n\ntool (\n\texample.com/other/cmd\n\tgithub.com/go-apis/loom/cmd/loom\n)\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, got := initIn(t, gomod)
			if got != gomod {
				t.Fatalf("go.mod changed:\n%s", got)
			}
		})
	}
}

func TestInitLeavesOldGoModAlone(t *testing.T) {
	gomod := "module x\n\ngo 1.22\n"
	_, got := initIn(t, gomod)
	if got != gomod {
		t.Fatalf("go.mod changed:\n%s", got)
	}
}

func TestInitWithoutGoMod(t *testing.T) {
	initIn(t, "")
	if _, err := os.Stat("go.mod"); err == nil {
		t.Fatal("init created a go.mod")
	}
	if _, err := os.Stat("loom.yml"); err != nil {
		t.Fatal(err)
	}
}
