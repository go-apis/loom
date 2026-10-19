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
	want := []string{"cart_gen.go", "models_gen.go", "registry_gen.go", "shipment_gen.go", "wallet_gen.go"}
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
	}
	shared := map[string][]string{
		"models_gen.go":   {"type Tier string", "type Address struct", "type Shared struct", "type CartView struct", "func (s *CartView) Fold("},
		"registry_gen.go": {"type ShipOnAddReactions interface", "type Impl struct", "func NewRegistry(", "func asAny[", "func loomErrNilEvent("},
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

	// and it compiles: a module beside the generated code, loom replaced by this tree
	if testing.Short() {
		return
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	gomod := "module example.com/shop\n\ngo 1.24\n\nrequire github.com/go-apis/loom v0.0.0\n\nreplace github.com/go-apis/loom => " + root + "\n"
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
	tidy.Dir, tidy.Env = dir, append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off")
	if out, err := tidy.CombinedOutput(); err != nil {
		t.Fatalf("go mod tidy: %v\n%s", err, out)
	}
	vet := exec.Command("go", "vet", "./...")
	vet.Dir = dir
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
