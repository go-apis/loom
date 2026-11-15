package gen_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

const wireSchema = `
service shop

aggregate Cart {
  state { items: int }
  command AddItem { items: int! } -> ItemAdded
  event ItemAdded { items: int! }
}

record Wallet {
  state { balance: int }
  command Credit { amount: int! } -> WalletCredited
  event WalletCredited { amount: int! }
}

entity CartRow { items: int }

projection cartRow -> CartRow @fold {
  on ItemAdded
}

policy tidy {
  on ItemAdded -> Credit
}

process notify {
  on ItemAdded
}
`

// one more aggregate, for what adding a declaration touches
const wireSchemaPlusOrder = wireSchema + `
aggregate Order {
  state { n: int }
  command Place -> Placed
  event Placed { n: int! }
}
`

func wireFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(filepath.Join(dir, "wire"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, "wire", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = string(raw)
	}
	return out
}

func names(files map[string]string) string {
	var out []string
	for name := range files {
		out = append(out, name)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// TestWireByConvention: with wire on, every declaration gets a wiring
// file of its own that builds its implementation the way its kind's
// package offers — the type, or a New constructor, with the one
// dependency type the constructors share — and registry.go only builds
// the registry from them. A new declaration adds a file and edits none;
// a removed one loses its file; what no convention says is a Configure.
// The wired service compiles and builds its Impl.
func TestWireByConvention(t *testing.T) {
	const module = "example.test/shop"
	dir := t.TempDir()
	cfg := gen.Config{Dir: dir, Module: module, Package: "shop", Layout: "folders", Wire: "wire"}
	generate := func(src string) *gen.Result {
		t.Helper()
		s, err := sdl.Parse(src)
		if err != nil {
			t.Fatal(err)
		}
		res, err := gen.Generate(s, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	// from nothing: stubs, and a wiring file for each declaration
	generate(wireSchema)
	files := wireFiles(t, dir)
	if got, want := names(files), "cart_aggregate_gen.go cartrow_projection_gen.go notify_process_gen.go tidy_policy_gen.go wallet_record_gen.go wire_gen.go"; got != want {
		t.Fatalf("wire files:\n got %s\nwant %s", got, want)
	}
	for file, want := range map[string]string{
		"cart_aggregate_gen.go":     "h := &aggregates.Cart{}\n\t\tconfigure(h, deps)\n\t\timpl.Cart(h)",
		"wallet_record_gen.go":      "h := &records.Wallet{}\n\t\tconfigure(h, deps)\n\t\timpl.Wallet(h)",
		"tidy_policy_gen.go":        "h := &policies.Tidy{}\n\t\tconfigure(h, deps)\n\t\timpl.Tidy(h)",
		"notify_process_gen.go":     "h := &processes.Notify{}\n\t\tconfigure(h, deps)\n\t\timpl.Notify(h)",
		"cartrow_projection_gen.go": "h := &projections.CartRow{}\n\t\tconfigure(h, deps)\n\t\timpl.CartRow(h)",
		"wire_gen.go":               "type Deps = struct{}",
	} {
		if !strings.Contains(files[file], want) {
			t.Errorf("%s lacks %q:\n%s", file, want, files[file])
		}
	}
	registry, err := os.ReadFile(filepath.Join(dir, "registry.go"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(registry); !strings.Contains(got, "func NewRegistry(deps wire.Deps) *loom.Registry {\n\timpl := wire.Impl(deps)\n\treturn loomgen.NewRegistry(impl)\n}") || strings.Contains(got, "aggregates.") {
		t.Errorf("registry.go lists implementations, or does not build from wire:\n%s", got)
	}
	vetGenerated(t, dir, module)

	// a process grows a constructor that takes the service's
	// dependencies: its file calls it, and Deps is that type
	write := func(rel, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, rel), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("processes/notify.go", `package processes

import (
	"context"

	"github.com/go-apis/loom"

	"example.test/shop/loomgen"
)

// Services is what the processes are given.
type Services struct{ Greeting string }

type Notify struct{ Services *Services }

func NewNotify(s *Services) *Notify { return &Notify{Services: s} }

func (h *Notify) OnItemAdded(ctx context.Context, evt *loom.Event, data *loomgen.ItemAdded) ([]loom.Command, error) {
	return nil, nil
}
`)
	// and an aggregate has something only other code can give it
	write("aggregates/cart.go", `package aggregates

import (
	"context"

	"github.com/go-apis/loom"

	"example.test/shop/loomgen"
)

type Cart struct{ Limit int }

func (h *Cart) AddItem(ctx context.Context, state *loomgen.Cart, cmd *loomgen.AddItem) ([]loom.DomainEvent, error) {
	return nil, nil
}
`)
	write("wire/cart.go", `package wire

import "example.test/shop/aggregates"

func init() {
	Configure(func(c *aggregates.Cart, deps Deps) { c.Limit = len(deps.Greeting) })
}
`)
	before := wireFiles(t, dir)
	generate(wireSchema)
	files = wireFiles(t, dir)
	if !strings.Contains(files["notify_process_gen.go"], "h := processes.NewNotify(deps)") {
		t.Errorf("the process with a constructor is not built by it:\n%s", files["notify_process_gen.go"])
	}
	if !strings.Contains(files["wire_gen.go"], "type Deps = *processes.Services") || !strings.Contains(files["wire_gen.go"], `"example.test/shop/processes"`) {
		t.Errorf("Deps is not the constructor's dependency type:\n%s", files["wire_gen.go"])
	}
	if files["cart.go"] != before["cart.go"] {
		t.Error("generate rewrote a hand-written file of the wire package")
	}

	// the wired service builds its Impl: every implementation bound,
	// constructed with deps, and set up by Configure
	write("wire/wire_test.go", `package wire

import (
	"testing"

	"example.test/shop/loomgen"
	"example.test/shop/processes"
)

func TestImpl(t *testing.T) {
	greeting := ""
	Configure(func(n *processes.Notify, deps Deps) { greeting = n.Services.Greeting })
	impl := Impl(&processes.Services{Greeting: "hello"})
	if greeting != "hello" {
		t.Fatalf("the process was not built with deps: its greeting is %q", greeting)
	}
	// NewRegistry refuses an Impl with anything unbound: it answering
	// says the aggregate, record, policy, process and projection are
	reg := loomgen.NewRegistry(impl)
	if len(reg.Aggregates) != 1 || len(reg.Records) != 1 || len(reg.Policies) != 1 || len(reg.Processes) != 1 {
		t.Fatalf("registry: %d aggregates, %d records, %d policies, %d processes", len(reg.Aggregates), len(reg.Records), len(reg.Policies), len(reg.Processes))
	}
	folds := 0
	for _, p := range reg.Projections {
		if p.Fold != nil {
			folds++
		}
	}
	if folds != 1 {
		t.Fatalf("registry: %d projections with hand-written folds, want 1", folds)
	}
	if configured != 5 {
		t.Fatalf("Configure saw a *aggregates.Cart with Limit %d, want 5", configured)
	}
}
`)
	write("wire/seen.go", `package wire

import "example.test/shop/aggregates"

// configured is what the Cart's Limit was when it was bound: Configure
// runs before the binding.
var configured int

func init() {
	Configure(func(c *aggregates.Cart, _ Deps) { configured = c.Limit })
}
`)
	vetGenerated(t, dir, module)
	if !testing.Short() {
		run := exec.Command("go", "test", "./wire/")
		run.Dir, run.Env = dir, goEnv(t)
		if out, err := run.CombinedOutput(); err != nil {
			t.Fatalf("the wired service's own test: %v\n%s", err, out)
		}
	}

	// a new aggregate adds one wiring file and changes no other
	before = wireFiles(t, dir)
	generate(wireSchemaPlusOrder)
	after := wireFiles(t, dir)
	for name, content := range before {
		if after[name] != content {
			t.Errorf("adding an aggregate changed wire/%s", name)
		}
	}
	if len(after) != len(before)+1 || !strings.Contains(after["order_aggregate_gen.go"], "impl.Order(h)") {
		t.Errorf("adding an aggregate: wire files %s", names(after))
	}
	registryAfter, _ := os.ReadFile(filepath.Join(dir, "registry.go"))
	if string(registryAfter) != string(registry) {
		t.Error("adding an aggregate changed registry.go")
	}

	// Check names a missing and a stale wiring file, and one left behind
	s, _ := sdl.Parse(wireSchema)
	stale, missing, err := gen.Check(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var staleNames []string
	for _, f := range stale {
		staleNames = append(staleNames, filepath.Base(filepath.Dir(f))+"/"+filepath.Base(f))
	}
	if got := strings.Join(staleNames, " "); len(missing) != 0 || got != "loomgen/order_gen.go wire/order_aggregate_gen.go" {
		t.Errorf("check after the aggregate went: stale %q, missing %v; want its generated file and its wiring file", got, missing)
	}

	// removing the declaration removes its wiring file, and no hand file
	res := generate(wireSchema)
	if len(res.Removed) == 0 || !strings.Contains(strings.Join(res.Removed, " "), "order_aggregate_gen.go") {
		t.Errorf("removed = %v; want the departed aggregate's wiring file", res.Removed)
	}
	if got := wireFiles(t, dir); got["order_aggregate_gen.go"] != "" || got["cart.go"] == "" || got["seen.go"] == "" {
		t.Errorf("after removal: %s", names(got))
	}
}

// TestWireRefusesWhatItCannotConvention: wiring needs the folders
// layout, and constructors that share one dependency type.
func TestWireRefusesWhatItCannotConvention(t *testing.T) {
	s, err := sdl.Parse(wireSchema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gen.Generate(s, gen.Config{Dir: t.TempDir(), Module: "example.test/shop", Package: "shop", Wire: "wire"}); err == nil || !strings.Contains(err.Error(), "layout: folders") {
		t.Errorf("wire with the flat layout: %v", err)
	}

	dir := t.TempDir()
	cfg := gen.Config{Dir: dir, Module: "example.test/shop", Package: "shop", Layout: "folders", Wire: "wire"}
	if _, err := gen.Generate(s, cfg); err != nil {
		t.Fatal(err)
	}
	for rel, content := range map[string]string{
		"processes/deps.go": "package processes\n\ntype Services struct{}\n\nfunc NewNotify(s *Services) *Notify { return &Notify{} }\n",
		"policies/deps.go":  "package policies\n\ntype Other struct{}\n\nfunc NewTidy(o *Other) *Tidy { return &Tidy{} }\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, rel), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := gen.Generate(s, cfg); err == nil || !strings.Contains(err.Error(), "share one dependency type") {
		t.Errorf("constructors with two dependency types: %v", err)
	}
}
