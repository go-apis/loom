package sdl_test

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

const valid = `
service orders

aggregate Order @snapshot(5) {
  state {
    status: string
    items: [OrderItem]
  }
  command PlaceOrder {
    items: [OrderItem]!
  } -> OrderPlaced
  event OrderPlaced @publish @v(2) {
    status: string!
    items: [OrderItem]!
  }
}

record Ledger {
  state {
    total_cents: int
    account_ref: string @pii
  }
  command PostLedger {
    amount_cents: int!
  } -> LedgerPosted
  command AdjustLedger {
    amount_cents: int!
  }
  event LedgerPosted {
    amount_cents: int!
  }
}

policy postOnPlace {
  on OrderPlaced -> PostLedger
}

entity OrderSummary {
  status: string
}

projection orderSummary -> OrderSummary {
  on OrderPlaced
}

policy noteLocally {
  on OrderPlaced -> PlaceOrder
}

process shipOnPayment {
  on billing.InvoicePaid -> PlaceOrder
  effect carrier_pickup
  effect notify_customer
}

consume billing.InvoicePaid {
  paid_at: timestamp
}

type OrderItem {
  sku: string!
  quantity: int!
}
`

func TestParse(t *testing.T) {
	s, err := sdl.Parse(valid)
	if err != nil {
		t.Fatal(err)
	}
	if s.Service != "orders" || len(s.Aggregates) != 1 || len(s.Types) != 1 {
		t.Fatalf("unexpected shape: %+v", s)
	}
	evt := s.FindEvent("OrderPlaced")
	if evt == nil || !evt.Publish || evt.Version != 2 {
		t.Fatalf("OrderPlaced misparsed: %+v", evt)
	}
	foreign := s.FindEvent("InvoicePaid")
	if foreign == nil || foreign.Service != "billing" || !foreign.Publish {
		t.Fatalf("foreign event misparsed: %+v", foreign)
	}
	if len(s.Policies) != 2 || len(s.Processes) != 1 || len(s.Projections) != 1 {
		t.Fatalf("reactors misparsed")
	}
	if len(s.Records) != 1 || len(s.Records[0].Commands) != 2 {
		t.Fatalf("record misparsed: %+v", s.Records)
	}
	// effects sort alphabetically on the process
	if fx := s.Processes[0].Effects; len(fx) != 2 || fx[0] != "carrier_pickup" || fx[1] != "notify_customer" {
		t.Fatalf("effects misparsed: %+v", fx)
	}
	if pii := s.Records[0].State.PIIFields(); len(pii) != 1 || pii[0] != "account_ref" {
		t.Fatalf("@pii misparsed: %+v", pii)
	}
	// record commands may emit nothing (AdjustLedger): the state write is
	// the effect
	if _, c := s.FindRecordCommand("AdjustLedger"); c == nil || len(c.Emits) != 0 {
		t.Fatalf("record command emit rules misparsed")
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]struct {
		src     string
		wantErr string
	}{
		"policy on foreign event": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
policy p { on other.F -> C }
consume other.F {}
`,
			wantErr: "policies run in the producing transaction",
		},
		"undeclared emit": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> Missing
}
`,
			wantErr: "undeclared event Missing",
		},
		"nested object literal": {
			src: `
service s
aggregate A {
  state { x: { y: string } }
  command C -> E
  event E { x: string }
}
`,
			wantErr: "nested object fields must use a declared type",
		},
		"command with no effect": {
			src: `
service s
aggregate A {
  state { x: string }
  command C
}
`,
			wantErr: "emits nothing",
		},
		"effect on a policy": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
policy p {
  on E -> C
  effect callout
}
`,
			wantErr: "cannot declare effects",
		},
		"pii on published event": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E @publish { tin: string @pii }
}
`,
			wantErr: "@publish and @pii are incompatible",
		},
		"pii on command": {
			src: `
service s
aggregate A {
  state { x: string }
  command C { tin: string @pii } -> E
  event E { x: string }
}
`,
			wantErr: "only valid on local unpublished events",
		},
		"pii inside named type": {
			src: `
service s
aggregate A {
  state { x: T }
  command C -> E
  event E { x: string }
}
type T { tin: string @pii }
`,
			wantErr: "only valid on local unpublished events",
		},
		"duplicate effect": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
process p {
  on E -> C
  effect callout
  effect callout
}
`,
			wantErr: "declares effect callout twice",
		},
	}
	for name, tc := range cases {
		_, err := sdl.Parse(tc.src)
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: want error containing %q, got %v", name, tc.wantErr, err)
		}
	}
}
