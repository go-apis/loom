package gen_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/schema"
	"github.com/go-apis/loom/sdl"
)

// perFileSchema has two aggregates and a record, an event two of them
// emit (shared), an enum and a type (shared), and a projection and a
// policy (shared wiring).
const perFileSchema = `
service shop

enum Tier { gold silver }

type Address { line: string! }

event Shared { note: string }

aggregate Cart {
  state {
    items: int
    tier: Tier
  }
  command AddItem {
    items: int!
    tier: Tier
  } -> ItemAdded
  command Touch -> Shared
  event ItemAdded {
    items: int!
    tier: Tier
  }
}

aggregate Shipment {
  state {
    address: Address
    sent: bool
  }
  command Send { address: Address! } -> ShipmentSent
  command Note -> Shared
  event ShipmentSent {
    address: Address!
    sent: bool
  }
}

record Wallet {
  state { balance: int }
  command Credit { amount: int! } -> WalletCredited
  event WalletCredited { amount: int! }
}

entity CartView {
  items: int
}

projection carts -> CartView {
  on ItemAdded
}

policy shipOnAdd {
  on ItemAdded -> Send
}
`

func parsePerFile(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := sdl.Parse(perFileSchema)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestGenerateWritesOneFilePerAggregate: each aggregate and record gets
// loomgen/<snake name>_gen.go holding its own state, commands, events,
// fold, Handlers interface and def function — and nothing of the others;
// what is shared stays in models_gen.go and registry_gen.go; the package
// compiles (ADR 0008).
func TestGenerateWritesOneFilePerAggregate(t *testing.T) {
	s := parsePerFile(t)
	dir := t.TempDir()
	if _, err := gen.Generate(s, gen.Config{Dir: dir, Package: "shop", Module: "example.com/shop"}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "loomgen"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name())
	}
	want := []string{"cart_gen.go", "models_gen.go", "registry_gen.go", "ship_on_add_policy_gen.go", "shipment_gen.go", "wallet_gen.go"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("loomgen holds %v, want %v", got, want)
	}

	read := func(name string) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, "loomgen", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	own := map[string][]string{
		"cart_gen.go": {
			"type Cart struct", "func (s *Cart) Fold(", "type AddItem struct", "func (c *AddItem) Validate() error",
			"type Touch struct", "type ItemAdded struct", "type CartHandlers interface", "func aggregateDefCart(h CartHandlers) *loom.AggregateDef",
		},
		"shipment_gen.go": {
			"type Shipment struct", "func (s *Shipment) Fold(", "type Send struct", "type Note struct",
			"type ShipmentSent struct", "type ShipmentHandlers interface", "func aggregateDefShipment(h ShipmentHandlers) *loom.AggregateDef",
		},
		"wallet_gen.go": {
			"type Wallet struct", "type Credit struct", "type WalletCredited struct",
			"type WalletHandlers interface", "func recordDefWallet(h WalletHandlers) *loom.RecordDef",
		},
		// the policy's interface, its binding method and its definition
		"ship_on_add_policy_gen.go": {
			"type ShipOnAddReactions interface", "func (impl *Impl) ShipOnAdd(h ShipOnAddReactions)", `Name:   "shipOnAdd"`,
		},
	}
	shared := map[string][]string{
		"models_gen.go":   {"type Tier string", "type Address struct", "type Shared struct", "type CartView struct", "func (s *CartView) Fold("},
		"registry_gen.go": {"type Impl struct", "func NewRegistry(", "func asAny[", "func loomErrNilEvent("},
	}
	for file, decls := range own {
		src := read(file)
		for _, d := range decls {
			if !strings.Contains(src, d) {
				t.Errorf("%s has no %q", file, d)
			}
		}
		// none of another aggregate's or record's declarations, nor a shared one
		for other, theirs := range own {
			if other == file {
				continue
			}
			for _, d := range theirs {
				if strings.Contains(src, d) {
					t.Errorf("%s has %s's %q", file, other, d)
				}
			}
		}
		for other, theirs := range shared {
			for _, d := range theirs {
				if strings.Contains(src, d) {
					t.Errorf("%s has %s's %q", file, other, d)
				}
			}
		}
	}
	for file, decls := range shared {
		src := read(file)
		for _, d := range decls {
			if !strings.Contains(src, d) {
				t.Errorf("%s has no %q", file, d)
			}
		}
		for other, theirs := range own {
			for _, d := range theirs {
				if strings.Contains(src, d) {
					t.Errorf("%s has %s's %q", file, other, d)
				}
			}
		}
	}

	// and it compiles
	vetGenerated(t, dir, "example.com/shop")
}

// vetGenerated runs go vet over a generated service in dir: a module
// beside the generated code, loom replaced by this tree. Skipped with
// -short.
func vetGenerated(t *testing.T, dir, module string) {
	t.Helper()
	if testing.Short() {
		return
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	gomod := "module " + module + "\n\ngo 1.24\n\nrequire github.com/go-apis/loom v0.0.0\n\nreplace github.com/go-apis/loom => " + root + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatal(err)
	}
	sum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go.sum"), sum, 0o644); err != nil {
		t.Fatal(err)
	}
	tidy := exec.Command("go", "mod", "tidy")
	// tidy walks the whole module graph, including go.mod files of old
	// versions a build never fetches; on a cold module cache it must be
	// allowed to download them, so the environment's GOPROXY stands
	// (goEnv: a writable module cache even where the inherited one is
	// read-only; see goenv_test.go)
	tidy.Dir, tidy.Env = dir, goEnv(t, "-mod=mod")
	if out, err := tidy.CombinedOutput(); err != nil {
		t.Fatalf("go mod tidy: %v\n%s", err, out)
	}
	vet := exec.Command("go", "vet", "./...")
	vet.Dir, vet.Env = dir, goEnv(t)
	if out, err := vet.CombinedOutput(); err != nil {
		t.Fatalf("the generated package does not compile: %v\n%s", err, out)
	}
}

// TestGenerateIsDeterministic: rendering the same schema twice, and
// generating it into two directories, gives byte-identical file sets — no
// map iteration order reaches the output (ADR 0008).
func TestGenerateIsDeterministic(t *testing.T) {
	cfg := gen.Config{Dir: "/svc", Package: "shop", Module: "example.com/shop"}
	first, err := gen.Render(parsePerFile(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		again, err := gen.Render(parsePerFile(t), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(again) != len(first) {
			t.Fatalf("render %d: %d files, first had %d", i, len(again), len(first))
		}
		for path, want := range first {
			if !bytes.Equal(again[path], want) {
				t.Fatalf("render %d: %s differs", i, path)
			}
		}
	}

	tree := func() map[string][]byte {
		t.Helper()
		dir := t.TempDir()
		if _, err := gen.Generate(parsePerFile(t), gen.Config{Dir: dir, Package: "shop", Module: "example.com/shop"}); err != nil {
			t.Fatal(err)
		}
		out := map[string][]byte{}
		err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			rel, _ := filepath.Rel(dir, p)
			out[rel], err = os.ReadFile(p)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	a, b := tree(), tree()
	var names []string
	for rel := range a {
		names = append(names, rel)
	}
	sort.Strings(names)
	if len(a) != len(b) {
		t.Fatalf("two generates wrote %d and %d files", len(a), len(b))
	}
	for _, rel := range names {
		if !bytes.Equal(a[rel], b[rel]) {
			t.Fatalf("%s differs between two generates", rel)
		}
	}
	if len(names) == 0 || !strings.Contains(strings.Join(names, " "), filepath.Join("loomgen", "cart_gen.go")) {
		t.Fatalf("generate wrote %v", names)
	}
}

// An aggregate whose file would be a shared one — `aggregate Registry`
// would write registry_gen.go — takes its kind appended instead, so it
// never overwrites the shared file.
func TestGenerateAggregateNamedLikeASharedFile(t *testing.T) {
	s, err := sdl.Parse(`
service s
aggregate Registry {
  state { x: string }
  command Open -> Opened
  event Opened { x: string }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	files, err := gen.Render(s, gen.Config{Dir: "/svc", Package: "s", Module: "example.com/s"})
	if err != nil {
		t.Fatal(err)
	}
	own := files[filepath.Join("/svc", "loomgen", "registry_aggregate_gen.go")]
	if !bytes.Contains(own, []byte("type RegistryHandlers interface")) {
		t.Fatalf("registry_aggregate_gen.go has no RegistryHandlers: %s", own)
	}
	if !bytes.Contains(files[filepath.Join("/svc", "loomgen", "registry_gen.go")], []byte("func NewRegistry(")) {
		t.Fatal("registry_gen.go lost NewRegistry")
	}
}

// Two features, each adding an aggregate in its own .loom file.
const (
	addedAccount = `
service shop

aggregate Account {
  state {
    owner: string
    tier: Tier
  }
  command OpenAccount { owner: string! tier: Tier } -> AccountOpened
  command CloseAccount -> AccountClosed
  event AccountOpened { owner: string! tier: Tier }
  event AccountClosed { at: string }
}
`
	addedBasket = `
service shop

aggregate Basket {
  state {
    address: Address
    lines: int
  }
  command FillBasket { lines: int! address: Address } -> BasketFilled
  event BasketFilled { lines: int! address: Address }
}
`
)

// TestTwoAggregatesAddedTouchDisjointFiles: two branches that each add an
// aggregate change no generated file the base already has, and add
// disjoint files — so they cannot conflict in loomgen. Merged, they
// build (ADR 0008).
func TestTwoAggregatesAddedTouchDisjointFiles(t *testing.T) {
	cfg := gen.Config{Dir: "/svc", Package: "shop", Module: "example.com/shop"}
	parse := func(extra ...string) *schema.Schema {
		t.Helper()
		files := []sdl.File{{Path: "shop.loom", Src: perFileSchema}}
		for i, src := range extra {
			files = append(files, sdl.File{Path: string(rune('a'+i)) + ".loom", Src: src})
		}
		s, err := sdl.ParseFiles(files)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	render := func(s *schema.Schema) map[string][]byte {
		t.Helper()
		files, err := gen.Render(s, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	base := render(parse())
	withA := render(parse(addedAccount))
	withB := render(parse(addedBasket))

	added := func(name string, files map[string][]byte) []string {
		t.Helper()
		var out []string
		for path := range files {
			if _, ok := base[path]; !ok {
				out = append(out, filepath.Base(path))
			}
		}
		sort.Strings(out)
		for path, want := range base {
			got, ok := files[path]
			if !ok {
				t.Errorf("base+%s dropped %s", name, filepath.Base(path))
			} else if !bytes.Equal(got, want) {
				t.Errorf("base+%s changed %s:\n%s", name, filepath.Base(path), got)
			}
		}
		return out
	}
	addsA, addsB := added("Account", withA), added("Basket", withB)
	if strings.Join(addsA, " ") != "account_gen.go" || strings.Join(addsB, " ") != "basket_gen.go" {
		t.Fatalf("Account adds %v, Basket adds %v; want each its own file only", addsA, addsB)
	}
	for _, a := range addsA {
		for _, b := range addsB {
			if a == b {
				t.Fatalf("both add %s", a)
			}
		}
	}

	// base+A+B: the union, each added file as its branch rendered it, and it builds
	both := render(parse(addedAccount, addedBasket))
	if len(both) != len(base)+len(addsA)+len(addsB) {
		t.Fatalf("base+A+B renders %d files, want %d", len(both), len(base)+len(addsA)+len(addsB))
	}
	for path, want := range withA {
		if !bytes.Equal(both[path], want) {
			t.Errorf("base+A+B differs from base+A in %s", filepath.Base(path))
		}
	}
	for path, want := range withB {
		if !bytes.Equal(both[path], want) {
			t.Errorf("base+A+B differs from base+B in %s", filepath.Base(path))
		}
	}
	dir := t.TempDir()
	if _, err := gen.Generate(parse(addedAccount, addedBasket), gen.Config{Dir: dir, Package: "shop", Module: "example.com/shop"}); err != nil {
		t.Fatal(err)
	}
	vetGenerated(t, dir, "example.com/shop")
}

// two processes, each declared in its own schema file, reacting to the
// same event
var twoProcessFiles = []sdl.File{
	{Path: "schema/door.loom", Src: `
service shop

aggregate Door {
  state { open: bool }
  command Open -> Opened
  command Lock -> Locked
  command Chime -> Chimed
  event Opened { open: bool }
  event Locked { open: bool }
  event Chimed { open: bool }
}

process lockUp {
  on Opened -> Lock
}
`},
	{Path: "schema/ring.loom", Src: `
process ring {
  on Opened -> Chime
}
`},
}

// TestTwoProcessesInSeparateFilesBothReact: two processes declared in
// separate schema files each get a generated file of their own, holding
// their Reactions interface, their binding method and their definition,
// which no other generated file holds. Built into a registry, each
// reacts to its event with its declared command.
func TestTwoProcessesInSeparateFilesBothReact(t *testing.T) {
	s, err := sdl.ParseFiles(twoProcessFiles)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if _, err := gen.Generate(s, gen.Config{Dir: dir, Package: "shop", Module: "example.com/shop"}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "loomgen"))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, "loomgen", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = string(raw)
	}
	for file, decls := range map[string][]string{
		"lock_up_process_gen.go": {"type LockUpReactions interface", "func (impl *Impl) LockUp(h LockUpReactions)", `Name:   "lockUp"`},
		"ring_process_gen.go":    {"type RingReactions interface", "func (impl *Impl) Ring(h RingReactions)", `Name:   "ring"`},
	} {
		for _, decl := range decls {
			if !strings.Contains(files[file], decl) {
				t.Errorf("%s lacks %q", file, decl)
			}
			for other, content := range files {
				if other != file && strings.Contains(content, decl) {
					t.Errorf("%q is in %s as well as %s", decl, other, file)
				}
			}
		}
	}
	// nothing shared names either process
	for _, shared := range []string{"registry_gen.go", "models_gen.go"} {
		for _, name := range []string{"LockUp", "lockUp", "Ring", `"ring"`} {
			if strings.Contains(files[shared], name) {
				t.Errorf("%s names %s", shared, name)
			}
		}
	}
	if testing.Short() {
		return
	}

	// the implementations, in place of the stubs, and a test that reacts
	write := func(name, src string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reaction := func(typ, cmd string) string {
		return `package shop

import (
	"context"

	"github.com/go-apis/loom"

	"example.com/shop/loomgen"
)

type ` + typ + ` struct{}

func (*` + typ + `) OnOpened(ctx context.Context, evt *loom.Event, data *loomgen.Opened) ([]loom.Command, error) {
	return []loom.Command{&loomgen.` + cmd + `{CommandBase: loom.CommandBase{Namespace: evt.Namespace, AggregateID: evt.AggregateID}}}, nil
}
`
	}
	write("lockup.go", reaction("LockUp", "Lock"))
	write("ring.go", reaction("Ring", "Chime"))
	write("shop_test.go", `package shop

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/loomtest"

	"example.com/shop/loomgen"
)

func TestBothProcessesReact(t *testing.T) {
	reg := NewRegistry()
	if len(reg.Processes) != 2 || reg.Processes[0].Name != "lockUp" || reg.Processes[1].Name != "ring" {
		t.Fatalf("processes: %+v", reg.Processes)
	}
	door := uuid.New()
	base := loom.CommandBase{Namespace: "acme", AggregateID: door}
	loomtest.Reaction(t, reg, "lockUp").When("acme", door, &loomgen.Opened{Open: true}).Then(&loomgen.Lock{CommandBase: base})
	loomtest.Reaction(t, reg, "ring").When("acme", door, &loomgen.Opened{Open: true}).Then(&loomgen.Chime{CommandBase: base})
}

// a process never bound is named when the registry is built
func TestAnUnboundProcessIsNamed(t *testing.T) {
	defer func() {
		msg, _ := recover().(string)
		if !strings.Contains(msg, "process ring has no implementation") || !strings.Contains(msg, "impl.Ring(") {
			t.Fatalf("want a panic naming ring, got %q", msg)
		}
	}()
	impl := loomgen.Impl{}
	impl.Door(&Door{})
	impl.LockUp(&LockUp{})
	loomgen.NewRegistry(impl)
}
`)
	vetGenerated(t, dir, "example.com/shop")
	run := exec.Command("go", "test", "./...")
	run.Dir, run.Env = dir, goEnv(t)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("the generated service's tests: %v\n%s", err, out)
	}
}
