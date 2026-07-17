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
  command AttachContract {
    contract: file!
  } -> ContractAttached
  event OrderPlaced @publish @v(2) {
    status: string!
    customer_id: uuid!
    items: [OrderItem]!
  }
  event ContractAttached {
    contract: file!
  }
  upload Contract {
    on uploaded -> AttachContract
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

entity OrderSummary @table {
  status: string
}

projection orderSummary -> OrderSummary {
  on OrderPlaced
}

entity Spend {
  order_count: int
}

projection spend -> Spend @fold {
  on OrderPlaced key(customer_id)
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
	if len(s.Policies) != 2 || len(s.Processes) != 1 || len(s.Projections) != 2 {
		t.Fatalf("reactors misparsed")
	}
	// projections sort by name: [orderSummary, spend]
	if spend := s.Projections[1]; !spend.Fold || spend.Subscriptions[0].Key != "customer_id" {
		t.Fatalf("@fold/key misparsed: %+v", spend)
	}
	if plain := s.Projections[0]; plain.Fold || plain.Subscriptions[0].Key != "" {
		t.Fatalf("plain projection contaminated: %+v", plain)
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
	// entities sort by name: [OrderSummary, Spend]
	if !s.Entities[0].Table || s.Entities[1].Table {
		t.Fatalf("@table misparsed: %+v %+v", s.Entities[0], s.Entities[1])
	}
	if ups := s.Aggregates[0].Uploads; len(ups) != 1 || ups[0].Name != "Contract" || ups[0].OnUploaded != "AttachContract" || ups[0].OnStarted != "" {
		t.Fatalf("upload misparsed: %+v", s.Aggregates[0].Uploads)
	}
	if _, c := s.FindCommand("AttachContract"); c == nil || c.FileField() != "contract" {
		t.Fatalf("file field misparsed")
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
			wantErr: "only valid on commands, local unpublished events",
		},
		"table field shadows meta column": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
entity Sum @table {
  namespace: string
}
projection sum -> Sum { on E }
`,
			wantErr: "collides with a meta column",
		},
		"table with pii": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { tin: string @pii }
}
entity Sum @table {
  tin: string @pii
}
projection sum -> Sum { on E }
`,
			wantErr: "@table and @pii are incompatible",
		},
		"upload without uploaded hook": {
			src: `
service s
aggregate A {
  state { x: string }
  command C { doc: file! } -> E
  event E { doc: file! }
  upload Doc {
    on started -> C
  }
}
`,
			wantErr: "no `on uploaded` command",
		},
		"upload command without file field": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
  upload Doc {
    on uploaded -> C
  }
}
`,
			wantErr: "exactly one payload field, required and typed file",
		},
		"upload command on another owner": {
			src: `
service s
aggregate A {
  state { x: string }
  command C { doc: file! } -> E
  event E { doc: file! }
}
aggregate B {
  state { x: string }
  command D { doc: file! } -> E
  upload Doc {
    on uploaded -> C
  }
}
`,
			wantErr: "not a command of B",
		},
		"projection key on missing field": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
entity Y { x: string }
projection p -> Y {
  on E key(parent_id)
}
`,
			wantErr: "key(parent_id) is not a field of event E",
		},
		"projection key on non-uuid field": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { parent_id: string! }
}
entity Y { x: string }
projection p -> Y {
  on E key(parent_id)
}
`,
			wantErr: "must be a required uuid field",
		},
		"projection key on optional field": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { parent_id: uuid }
}
entity Y { x: string }
projection p -> Y {
  on E key(parent_id)
}
`,
			wantErr: "must be a required uuid field",
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
