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

// renaming an aggregate orphans its old file: render no longer produces
// loomgen/order_gen.go once the schema's Order becomes Shipment.
const renamedCheckSchema = `service orders

event OrderPlaced {
  status: string!
}

aggregate Shipment {
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

func TestGenerateRemovesOrphanedAggregateFile(t *testing.T) {
	dir, write, _, _ := checkFixture(t)
	if _, err := os.Stat(filepath.Join(dir, "loomgen", "order_gen.go")); err != nil {
		t.Fatalf("fixture missing order_gen.go: %v", err)
	}
	write("schema/orders.loom", renamedCheckSchema)

	var out bytes.Buffer
	if err := runGenerate([]string{"--dir", dir}, &out); err != nil {
		t.Fatalf("generate: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "removed loomgen/order_gen.go\n") {
		t.Fatalf("want removed loomgen/order_gen.go, got %q", out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "loomgen", "order_gen.go")); !os.IsNotExist(err) {
		t.Fatal("orphaned order_gen.go was not removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "loomgen", "shipment_gen.go")); err != nil {
		t.Fatalf("shipment_gen.go not written: %v", err)
	}
}

func TestGenerateCheckNamesOrphan(t *testing.T) {
	dir, write, _, check := checkFixture(t)
	write("schema/orders.loom", renamedCheckSchema)

	out, err := check()
	if err == nil {
		t.Fatal("want --check to fail on an orphaned file")
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	sort.Strings(lines)
	// renaming Order to Shipment touches no shared file (ADR 0008): the
	// orphan, loomgen/order_gen.go, is named stale although render no
	// longer produces it at all.
	want := []string{"missing loomgen/shipment_gen.go", "stale   loomgen/order_gen.go"}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("got %v, want %v", lines, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "loomgen", "order_gen.go")); err != nil {
		t.Fatal("--check removed the orphaned file")
	}
}

func TestGenerateKeepsUnheaderedFile(t *testing.T) {
	dir, write, read, check := checkFixture(t)
	write("loomgen/mystery_gen.go", "package loomgen\n\n// hand-written, no loom header\n")
	write("schema/orders.loom", renamedCheckSchema)

	out, err := check()
	if err == nil || strings.Contains(out, "mystery_gen.go") {
		t.Fatalf("unheadered file should not be named: err %v, out %q", err, out)
	}

	var genOut bytes.Buffer
	if err := runGenerate([]string{"--dir", dir}, &genOut); err != nil {
		t.Fatalf("generate: %v\n%s", err, genOut.String())
	}
	if strings.Contains(genOut.String(), "mystery_gen.go") {
		t.Fatalf("generate touched the unheadered file: %q", genOut.String())
	}
	if read("loomgen/mystery_gen.go") != "package loomgen\n\n// hand-written, no loom header\n" {
		t.Fatal("unheadered file was rewritten or removed")
	}
}

func TestGenerateCheckMissing(t *testing.T) {
	// a shared file and an aggregate's own file (ADR 0008) are both checked
	for _, rel := range []string{"loomgen/models_gen.go", "loomgen/order_gen.go"} {
		dir, _, _, check := checkFixture(t)
		p := filepath.Join(dir, rel)
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		out, err := check()
		if err == nil || strings.TrimSpace(out) != "missing "+rel {
			t.Fatalf("err %v, out %q", err, out)
		}
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatal("--check recreated the file")
		}
	}
}

func TestGenerateCheckStaleAggregateFile(t *testing.T) {
	_, write, _, check := checkFixture(t)
	// a new command on Order changes Order's file only
	write("schema/orders.loom", strings.Replace(checkSchema, "  command CancelOrder {", "  command ReopenOrder -> OrderPlaced\n  command CancelOrder {", 1))
	out, err := check()
	if err == nil || strings.TrimSpace(out) != "stale   loomgen/order_gen.go" {
		t.Fatalf("err %v, out %q", err, out)
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

// loom skill writes .claude/skills/loom/SKILL.md at the repository root —
// found by walking up to .git from a service directory below it — stamped
// with the loom version; --check passes on that file and fails, naming
// the command to run, when it is missing, hand-edited, or from another
// version (ADR 0007).
func TestSkillWritesAndChecks(t *testing.T) {
	versionOverride = "v9.9.9"
	t.Cleanup(func() { versionOverride = "" })
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	svc := filepath.Join(root, "platform")
	if err := os.MkdirAll(svc, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(svc)

	if err := runSkill([]string{"--check"}, io.Discard); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("check with no skill: %v", err)
	}
	if err := runSkill(nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".claude", "skills", "loom", "SKILL.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the skill is not at the repository root: %v", err)
	}
	for _, want := range []string{"name: loom", "description: ", `loom-version: "v9.9.9"`, "go tool loom generate", "Never hand-edit `loomgen/`", "Never name a reactor after an aggregate", "loom.Once"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("the skill has no %q", want)
		}
	}
	if strings.Contains(string(raw), "{{LOOM_VERSION}}") {
		t.Fatal("the version placeholder was left in")
	}
	if err := runSkill([]string{"--check"}, io.Discard); err != nil {
		t.Fatalf("check on the written skill: %v", err)
	}
	// hand-edited, then another version: both stale
	if err := os.WriteFile(path, append(raw, []byte("\nmy note\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runSkill([]string{"--check"}, io.Discard); err == nil || !strings.Contains(err.Error(), "go tool loom skill") {
		t.Fatalf("check on an edited skill: %v", err)
	}
	if err := runSkill(nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	versionOverride = "v9.9.10"
	if err := runSkill([]string{"--check"}, io.Discard); err == nil || !strings.Contains(err.Error(), "v9.9.10") {
		t.Fatalf("check after a bump: %v", err)
	}
	// --root wins over the walk
	other := t.TempDir()
	if err := runSkill([]string{"--root", other}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(other, ".claude", "skills", "loom", "SKILL.md")); err != nil {
		t.Fatal(err)
	}
}

// loom init writes the skill too, and leaves one the repository already
// carries alone.
func TestInitWritesTheSkillOnce(t *testing.T) {
	versionOverride = "v9.9.9"
	t.Cleanup(func() { versionOverride = "" })
	dir, _ := initIn(t, "module example.com/svc\n\ngo 1.24\n")
	path := filepath.Join(dir, ".claude", "skills", "loom", "SKILL.md")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("init wrote no skill: %v", err)
	}
	if err := os.WriteFile(path, []byte("ours\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "loom.yml")); err != nil {
		t.Fatal(err)
	}
	if err := runInit([]string{"svc"}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "ours\n" {
		t.Fatalf("init overwrote the repository's skill: %q", b)
	}
}

// a @table entity's declared indexes reach loomgen/registry_gen.go (its
// TableDef) and loomgen/tables_gen.sql; --check must catch a schema/registry
// mismatch that is only an index, naming both stale files and writing
// nothing.
const indexCheckSchema = `service orders

entity WorkItem @table {
  org_id: uuid
  status: string
}
`

func TestGenerateCheckFailsOnIndexMismatch(t *testing.T) {
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
	write("go.mod", "module example.com/orders\n\ngo 1.22\n")
	write("loom.yml", "schema: schema\n")
	write("schema/orders.loom", indexCheckSchema)
	if err := runGenerate([]string{"--dir", dir}, io.Discard); err != nil {
		t.Fatal(err)
	}

	// declare an index without regenerating: the registry's TableDef and
	// tables_gen.sql both go stale
	withIndex := strings.Replace(indexCheckSchema, "  status: string\n}", "  status: string\n  index (status)\n}", 1)
	write("schema/orders.loom", withIndex)

	before := snapshot(t, dir)
	var out bytes.Buffer
	err := runGenerate([]string{"--dir", dir, "--check"}, &out)
	if err == nil {
		t.Fatal("want --check to fail on an index-only mismatch")
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	sort.Strings(lines)
	want := []string{"stale   loomgen/registry_gen.go", "stale   loomgen/tables_gen.sql"}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("got %v, want %v", lines, want)
	}
	if !reflect.DeepEqual(before, snapshot(t, dir)) {
		t.Fatal("--check wrote files")
	}
}
