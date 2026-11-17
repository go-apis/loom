package gen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// declaredSchema has an aggregate that declares an event no command
// emits any more (OrderDispatched: the old ShipOrder emitted it, and its
// rows are still in the log), a @retired one, and an event declared
// outside every aggregate that nothing emits either.
const declaredSchema = `service orders

event Noted { status: string! }

aggregate Order {
  state {
    status: string
    shipped_at: timestamp?
  }
  command PlaceOrder -> OrderPlaced
  event OrderPlaced { status: string! }

  // nothing emits this any more; stored rows of it still set the status
  event OrderDispatched {
    status: string!
    shipped_at: timestamp!
  }

  event Gone @retired { status: string! }
}
`

// TestAnEventNothingEmitsStillFolds: an event declared in an aggregate's
// block keeps its case in that aggregate's Fold when no command emits it,
// so deleting the last command that emitted it leaves the stored rows
// folding as before. A @retired event folds nothing, and an event
// declared outside every aggregate is folded by none.
func TestAnEventNothingEmitsStillFolds(t *testing.T) {
	s, err := sdl.Parse(declaredSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("an aggregate declaring an event nothing emits is valid: %v", err)
	}
	if got := strings.Join(s.Aggregates[0].Events, " "); got != "OrderPlaced OrderDispatched Gone" {
		t.Fatalf("the events Order declares: got %q", got)
	}
	dir := t.TempDir()
	if _, err := gen.Generate(s, gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders"}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "loomgen", "order_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	order := string(raw)
	fold := order[strings.Index(order, "func (s *Order) Fold("):]
	fold = fold[:strings.Index(fold, "\n}\n")]
	if !strings.Contains(fold, "case *OrderPlaced:") {
		t.Errorf("Order's Fold lost OrderPlaced:\n%s", fold)
	}
	if !strings.Contains(fold, "case *OrderDispatched:\n\t\tvShippedAt := e.ShippedAt\n\t\ts.ShippedAt = &vShippedAt\n\t\ts.Status = e.Status\n") {
		t.Errorf("Order's Fold dropped OrderDispatched, an event it declares that nothing emits:\n%s", fold)
	}
	if strings.Contains(fold, "case *Gone:") {
		t.Errorf("a @retired event folds nothing:\n%s", fold)
	}
	if strings.Contains(fold, "case *Noted:") {
		t.Errorf("an event declared outside Order folds into it:\n%s", fold)
	}
	if testing.Short() {
		return
	}
	vetGenerated(t, dir, "example.com/orders")
}
