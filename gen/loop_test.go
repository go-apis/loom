package gen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// TestLoopEmitsLookInterfaceAndStub: an aggregate declaring @loop gets a
// method on the service-wide Look interface and a once-only stub, mirroring
// EventUpcasts/Upcasts (ADR 0012). A second generate on the regenerated
// fixture is clean.
func TestLoopEmitsLookInterfaceAndStub(t *testing.T) {
	s, err := sdl.Parse(`service orders

aggregate Order @loop {
  state { status: string }
  command PlaceOrder -> OrderPlaced
  event OrderPlaced { status: string! }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders"}
	if _, err := gen.Generate(s, cfg); err != nil {
		t.Fatal(err)
	}

	registry, err := os.ReadFile(filepath.Join(dir, "loomgen", "look_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(registry), "type Look interface {") {
		t.Errorf("look_gen.go missing the Look interface")
	}
	if !strings.Contains(string(registry), "LookOrder(ctx context.Context) ([]loom.Command, error)") {
		t.Errorf("Look interface missing LookOrder: %s", registry)
	}
	scaffold, err := os.ReadFile(filepath.Join(dir, "loomgen", "loom_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(scaffold), "Look Look\n") {
		t.Errorf("Impl missing a Look field")
	}
	if !strings.Contains(string(scaffold), `panic("loomgen: the schema declares @loop`) {
		t.Errorf("NewRegistry missing the @loop nil-check panic")
	}

	stub, err := os.ReadFile(filepath.Join(dir, "look.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stub), "type Look struct{}") {
		t.Errorf("look.go missing the Look stub type: %s", stub)
	}
	if !strings.Contains(string(stub), "func (l *Look) LookOrder(ctx context.Context) ([]loom.Command, error) {") {
		t.Errorf("look.go missing LookOrder: %s", stub)
	}

	// a second generate does not overwrite the hand-owned stub
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

// TestLoopAbsentOmitsLookInterface: a schema with no @loop aggregate gets no
// Look interface, no Impl.Look field, and no stub.
func TestLoopAbsentOmitsLookInterface(t *testing.T) {
	s, err := sdl.Parse(`service orders

aggregate Order {
  state { status: string }
  command PlaceOrder -> OrderPlaced
  event OrderPlaced { status: string! }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders"}
	if _, err := gen.Generate(s, cfg); err != nil {
		t.Fatal(err)
	}
	registry, err := os.ReadFile(filepath.Join(dir, "loomgen", "loom_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(registry), "Look") {
		t.Errorf("no @loop aggregate declared, but loom_gen.go mentions Look: %s", registry)
	}
	if _, err := os.Stat(filepath.Join(dir, "look.go")); !os.IsNotExist(err) {
		t.Errorf("no @loop aggregate declared, but look.go was written")
	}
}
