package gen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// TestAgentGeneratesItsDefAndAsk: a declared agent is written to the
// aggregate's registry entry (kind and deadline) and given a typed ask
// and a name constant beside the Look interface.
func TestAgentGeneratesItsDefAndAsk(t *testing.T) {
	s, err := sdl.Parse(`service orders

aggregate Order @loop {
  state { status: string }
  command PlaceOrder -> OrderPlaced
  agent triage { kind: "order-triage", deadline: 30m }
  event OrderPlaced { status: string! }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if _, err := gen.Generate(s, gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders"}); err != nil {
		t.Fatal(err)
	}
	agg, err := os.ReadFile(filepath.Join(dir, "loomgen", "order_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(agg), `{Name: "triage", Kind: "order-triage", Deadline: 1800000000000},`) {
		t.Errorf("order_gen.go has no AgentDef for triage:\n%s", agg)
	}
	look, err := os.ReadFile(filepath.Join(dir, "loomgen", "look_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`const OrderTriageAgent = "triage"`, "func AskOrderTriage(ctx context.Context, ask loom.AgentAsk) error {"} {
		if !strings.Contains(string(look), want) {
			t.Errorf("look_gen.go lacks %q:\n%s", want, look)
		}
	}
}
