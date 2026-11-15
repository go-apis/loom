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
