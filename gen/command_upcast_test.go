package gen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// TestCommandUpcastEmitsInterfaceAndStub: `upcast X @from(n)` on a
// command emits what an event's does — a method on a service-wide
// interface (CommandUpcasts, beside EventUpcasts), an Impl field with its
// nil-check, a once-only stub wired by registry.go — and binds the hop
// and the @v into the command's def.
func TestCommandUpcastEmitsInterfaceAndStub(t *testing.T) {
	s, err := sdl.Parse(`service orders

aggregate Order {
  state { status: string }
  command PlaceOrder -> OrderPlaced
  command CancelOrder @v(2) { reason: string } -> OrderPlaced
  event OrderPlaced { status: string! }
}

upcast CancelOrder @from(1)
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders"}
	if _, err := gen.Generate(s, cfg); err != nil {
		t.Fatal(err)
	}
	read := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	for path, wants := range map[string][]string{
		"loomgen/upcasts_gen.go": {"type CommandUpcasts interface {", "CancelOrderFromV1(data []byte) ([]byte, error)"},
		"loomgen/loom_gen.go":    {"CommandUpcasts CommandUpcasts\n", `panic("loomgen: the schema declares command upcasts`},
		"loomgen/order_gen.go": {
			"SchemaVersion: 2,",
			"def.Commands[0].Upcasts = map[int]loom.UpcastFunc{1: impl.CommandUpcasts.CancelOrderFromV1} // CancelOrder",
		},
		"command_upcasts.go": {"type CommandUpcasts struct{}", "func (u *CommandUpcasts) CancelOrderFromV1(data []byte) ([]byte, error) {"},
		"registry.go":        {"CommandUpcasts: &CommandUpcasts{}"},
	} {
		got := read(path)
		for _, want := range wants {
			if !strings.Contains(got, want) {
				t.Errorf("%s missing %q:\n%s", path, want, got)
			}
		}
	}
	if got := read("loomgen/upcasts_gen.go"); strings.Contains(got, "EventUpcasts") {
		t.Errorf("no event upcast, yet upcasts_gen.go declares EventUpcasts:\n%s", got)
	}
	if got := read("loomgen/order_gen.go"); strings.Count(got, "SchemaVersion: ") != 2 { // the event's, and CancelOrder's
		t.Errorf("an unversioned command was given a SchemaVersion:\n%s", got)
	}

	if _, err := gen.Generate(s, cfg); err != nil {
		t.Fatal(err)
	}
	stale, missing, err := gen.Check(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 || len(missing) != 0 {
		t.Errorf("generate --check not clean: stale %v, missing %v", stale, missing)
	}
}
