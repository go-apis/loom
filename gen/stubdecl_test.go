package gen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// A stub is not written when its directory already declares the type in
// a file named otherwise (the Order aggregate implemented in model.go,
// not order.go): a second file declaring Order would not compile, and
// every regenerate would write it again. The file that holds it is
// reported as kept; a declaration nothing implements yet still gets its
// stub.
func TestStubSkippedWhenDeclaredInAnotherFile(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders", Layout: "folders"}
	if _, err := gen.Generate(s, cfg); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(dir, "aggregates", "order.go")
	moved := filepath.Join(dir, "aggregates", "model.go")
	if err := os.Rename(stub, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "processes", "shiponpayment.go")); err != nil {
		t.Fatal(err)
	}

	res, err := gen.Generate(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stub); err == nil {
		t.Fatalf("wrote %s beside %s, which declares Order already", stub, moved)
	}
	if !contains(res.Skipped, moved) {
		t.Fatalf("the file that implements Order is not reported kept: %v", res.Skipped)
	}
	if !contains(res.Written, filepath.Join(dir, "processes", "shiponpayment.go")) {
		t.Fatalf("a declaration nothing implements lost its stub: %v", res.Written)
	}
}

func contains(paths []string, want string) bool {
	for _, p := range paths {
		if p == want || strings.HasSuffix(p, string(filepath.Separator)+filepath.Base(want)) && filepath.Dir(p) == filepath.Dir(want) {
			return true
		}
	}
	return false
}

// A service that builds its registry in a package of its own (Tripline's
// store/registry.go calls loomgen.NewRegistry) gets no registry.go stub
// at the root: it would be dead code the next regenerate wrote again.
func TestRegistryStubSkippedWhenBuiltElsewhere(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders", Layout: "folders"}
	if _, err := gen.Generate(s, cfg); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "registry.go")
	src, err := os.ReadFile(root)
	if err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(dir, "store", "registry.go")
	if err := os.MkdirAll(filepath.Dir(store), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store, []byte(strings.Replace(string(src), "package orders", "package store", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}

	res, err := gen.Generate(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); err == nil {
		t.Fatalf("wrote %s though %s builds the registry", root, store)
	}
	if !contains(res.Skipped, store) {
		t.Fatalf("the file that builds the registry is not reported kept: %v", res.Skipped)
	}
}
