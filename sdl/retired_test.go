package sdl

import (
	"strings"
	"testing"
)

// TestRetiredEvent covers @retired: the declaration survives so stored
// rows still decode and fold, but nothing may produce it, subscribe to
// it, or upcast it ever again.
func TestRetiredEvent(t *testing.T) {
	const live = `service orders

aggregate Order {
  state {
    status: string
  }

  command PlaceOrder -> OrderPlaced

  event OrderPlaced {
    status: string!
  }

  // nothing emits this any more; old rows still decode and fold
  event Foo @retired {
    status: string!
  }
}
`
	s, err := Parse(live)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("a retired event nobody references is valid: %v", err)
	}
	if evt := s.FindEvent("Foo"); evt == nil || !evt.Retired {
		t.Fatalf("@retired did not reach the event: %+v", evt)
	}
	if evt := s.FindEvent("OrderPlaced"); evt == nil || evt.Retired {
		t.Fatalf("@retired leaked onto OrderPlaced: %+v", evt)
	}

	// each of these reaches a retired event by a route Validate must close
	for name, src := range map[string]string{
		"command emits": strings.Replace(live,
			"command PlaceOrder -> OrderPlaced",
			"command PlaceOrder -> Foo", 1),
		"record command emits": live + `
record Ledger {
  state { balance: int }
  command Post -> Foo
}
`,
		"policy subscribes": live + `
policy p {
  on Foo -> PlaceOrder
}
`,
		"process subscribes": live + `
process p {
  on Foo -> PlaceOrder
}
`,
		"projection subscribes": live + `
entity Summary {
  status: string
}

projection summary -> Summary {
  on Foo
}
`,
		"upcast": strings.Replace(live, "event Foo @retired {", "event Foo @retired @v(2) {", 1) + `
upcast Foo @from(1)
`,
	} {
		// Parse validates on the way out, so either return carries it
		s, err := Parse(src)
		if err == nil {
			err = s.Validate()
		}
		if err == nil {
			t.Errorf("%s: Validate accepted a reference to a @retired event", name)
			continue
		}
		if !strings.Contains(err.Error(), "@retired") {
			t.Errorf("%s: Validate error does not name @retired: %v", name, err)
		}
	}
}
