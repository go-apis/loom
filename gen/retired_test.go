package gen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// TestRetiredReachesRegistry: a @retired event still gets a struct and a
// fold (its stored rows must keep replaying), and the generated EventDef
// carries Retired so the registry — and Migrate's unknown-type check —
// know it is declared but unproducible.
func TestRetiredReachesRegistry(t *testing.T) {
	s, err := sdl.Parse(`service orders

aggregate Order {
  state { status: string }
  command PlaceOrder -> OrderPlaced
  event OrderPlaced { status: string! }
  event Gone @retired { status: string! }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if _, err := gen.Generate(s, gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders"}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "loomgen", "registry_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	if !strings.Contains(src, `{Name: "Gone", SchemaVersion: 1, Publish: false, Service: "", Retired: true,`) {
		t.Errorf("Retired missing from the generated EventDef for Gone")
	}
	if strings.Contains(src, `{Name: "OrderPlaced"`) && strings.Contains(src, `{Name: "OrderPlaced", SchemaVersion: 1, Publish: false, Service: "", Retired: true,`) {
		t.Errorf("Retired leaked onto a live event")
	}

	models, err := os.ReadFile(filepath.Join(dir, "loomgen", "models_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	// the struct survives so stored rows still decode. The fold does not
	// name it — folds are generated from what commands emit, and a retired
	// event is emitted by nothing — so replay decodes the row and folds
	// nothing, which is exactly what retiring means.
	if !strings.Contains(string(models), "type Gone struct") {
		t.Errorf("@retired dropped the event struct — stored rows would stop decoding")
	}
	if !strings.Contains(string(models), `func (*Gone) LoomEvent() string`) {
		t.Errorf("@retired dropped the event's LoomEvent marker")
	}
	if strings.Contains(string(models), "case *Gone:") {
		t.Errorf("a retired event should fold nothing — it can never be emitted")
	}
}
