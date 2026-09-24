package sdl_test

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

const valid = `
service orders

enum OrderStatus { placed shipped cancelled }

aggregate Order @snapshot(5) {
  state {
    status: string
    items: [OrderItem]
  }
  command PlaceOrder @role(owner, member) {
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
  command PostLedger @role(clerk) {
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
  customer_id: uuid
  join spend -> Spend via customer_id
  join invoices -> [billing.InvoiceSummary] via customer_id
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

series TickPrice @time(observed_at) @dim(sku, source) @key(external_id) {
  sku: string!
  source: string!
  external_id: string!
  observed_at: timestamp!
  price_cents: int!
  note: string
}

policy noteLocally {
  on OrderPlaced -> PlaceOrder
}

process shipOnPayment @from(origin) {
  on billing.InvoicePaid -> PlaceOrder
  effect carrier_pickup
  effect notify_customer @idempotent
}

consume billing.InvoicePaid {
  paid_at: timestamp
}

upcast OrderPlaced @from(1)

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
	if len(evt.Upcasts) != 1 || evt.Upcasts[0] != 1 {
		t.Fatalf("upcast misparsed: %+v", evt.Upcasts)
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
	// @from(origin): the explicit opt-in to replaying the whole log; a
	// process that says nothing starts at the head, recorded as ""
	if from := s.Processes[0].From; from != "origin" {
		t.Fatalf("process @from misparsed: %q", from)
	}
	if from := s.Policies[0].From; from != "" {
		t.Fatalf("policy grew a start position: %q", from)
	}
	// effects sort alphabetically on the process
	if fx := s.Processes[0].Effects; len(fx) != 2 || fx[0] != "carrier_pickup" || fx[1] != "notify_customer" {
		t.Fatalf("effects misparsed: %+v", fx)
	}
	if idem := s.Processes[0].Idempotent; len(idem) != 1 || idem[0] != "notify_customer" {
		t.Fatalf("@idempotent misparsed: %+v", idem)
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
	if len(s.Enums) != 1 || s.Enums[0].Name != "OrderStatus" || len(s.Enums[0].Values) != 3 || s.Enums[0].Values[1] != "shipped" {
		t.Fatalf("enum misparsed: %+v", s.Enums)
	}
	// declared joins: local single + cross-service list
	joins := s.Entities[0].Joins
	if len(joins) != 2 ||
		joins[0].Field != "spend" || joins[0].Service != "" || joins[0].Entity != "Spend" || joins[0].List || joins[0].Via != "customer_id" ||
		joins[1].Field != "invoices" || joins[1].Service != "billing" || joins[1].Entity != "InvoiceSummary" || !joins[1].List || joins[1].Via != "customer_id" {
		t.Fatalf("joins misparsed: %+v %+v", joins[0], joins[1])
	}
	if _, c := s.FindCommand("AttachContract"); c == nil || c.FileField() != "contract" {
		t.Fatalf("file field misparsed")
	}
	// @role gates: aggregate and record commands carry the role list;
	// unannotated commands carry none
	if _, c := s.FindCommand("PlaceOrder"); c == nil || len(c.Roles) != 2 || c.Roles[0] != "owner" || c.Roles[1] != "member" {
		t.Fatalf("@role misparsed: %+v", s.Aggregates[0].Commands)
	}
	if _, c := s.FindRecordCommand("PostLedger"); c == nil || len(c.Roles) != 1 || c.Roles[0] != "clerk" {
		t.Fatalf("record @role misparsed: %+v", c)
	}
	if _, c := s.FindCommand("AttachContract"); len(c.Roles) != 0 {
		t.Fatalf("unannotated command grew roles: %+v", c.Roles)
	}
	// series: @time/@dim keep declared order, @key overrides identity
	sr := s.FindSeries("TickPrice")
	if sr == nil || sr.Time != "observed_at" ||
		len(sr.Dims) != 2 || sr.Dims[0] != "sku" || sr.Dims[1] != "source" ||
		len(sr.Keys) != 1 || sr.Keys[0] != "external_id" {
		t.Fatalf("series misparsed: %+v", sr)
	}
	if id := sr.IdentityFields(); len(id) != 1 || id[0] != "external_id" {
		t.Fatalf("series identity misparsed: %+v", id)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]struct {
		src     string
		wantErr string
	}{
		"enum duplicate value": {
			src: `
service s
enum X { a a }
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
`,
			wantErr: "declares value a twice",
		},
		"enum collides with type": {
			src: `
service s
enum X { a }
type X { y: string! }
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
`,
			wantErr: "collides with type",
		},
		"join targets undeclared local entity": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { customer_id: uuid! }
}
entity P {
  customer_id: uuid
  join spend -> Missing via customer_id
}
projection p -> P { on E }
`,
			wantErr: "targets undeclared entity",
		},
		"join via unknown field": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { customer_id: uuid! }
}
entity P {
  customer_id: uuid
  join other -> P2 via nope
}
entity P2 { customer_id: uuid }
projection p -> P { on E }
projection p2 -> P2 { on E }
`,
			wantErr: "no such field on the entity",
		},
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
		"command with unknown directive": {
			src: `
service s
aggregate A {
  state { x: string }
  command C @admin -> E
  event E { x: string }
}
`,
			wantErr: "commands take @role",
		},
		"empty @role": {
			src: `
service s
aggregate A {
  state { x: string }
  command C @role -> E
  event E { x: string }
}
`,
			wantErr: "at least one role name",
		},
		"duplicate @role": {
			src: `
service s
aggregate A {
  state { x: string }
  command C @role(owner, owner) -> E
  event E { x: string }
}
`,
			wantErr: "declares @role owner twice",
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
		"secret on published event": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E @publish { key: string @secret }
}
`,
			wantErr: "@publish and @pii are incompatible",
		},
		"secret projected into entity": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { key: string @secret }
}
entity Ent {
  key: string @secret
}
projection ent -> Ent {
  on E
}
`,
			wantErr: "@secret fields cannot be projected into entities",
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
		"upcast from at or above current version": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E @v(2) { x: string }
}
upcast E @from(2)
`,
			wantErr: "must name a version below",
		},
		"upcast coverage gap": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E @v(3) { x: string }
}
upcast E @from(1)
`,
			wantErr: "coverage has a gap",
		},
		"upcast on unversioned event": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
upcast E @from(1)
`,
			wantErr: "must name a version below",
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
		"unknown effect directive": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
process p {
  on E -> C
  effect callout @retry
}
`,
			wantErr: "unknown directive @retry",
		},
		"series without @time": {
			src: `
service s
series P @dim(sku) {
  sku: string!
  at: timestamp!
}
`,
			wantErr: "needs @time",
		},
		"series dim not required": {
			src: `
service s
series P @time(at) @dim(sku) {
  sku: string
  at: timestamp!
}
`,
			wantErr: "@dim(sku) must be a required scalar field",
		},
		"series pii": {
			src: `
service s
series P @time(at) @dim(sku) {
  sku: string!
  at: timestamp!
  buyer: string @pii
}
`,
			wantErr: "cannot live in a series",
		},
		"series collides with entity": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
entity P { x: string }
projection p -> P { on E }
series P @time(at) @dim(sku) {
  sku: string!
  at: timestamp!
}
`,
			wantErr: "series P collides with entity P",
		},
		"policy declares a start position": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
policy p @from(head) { on E -> C }
`,
			wantErr: "has no start position",
		},
		"process start position is not a position": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
process p @from(yesterday) { on E -> C }
`,
			wantErr: "is not a start position",
		},
		"process start position takes one argument": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
process p @from(head, origin) { on E -> C }
`,
			wantErr: "@from wants one start position",
		},
		"process takes no other directive": {
			src: `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
process p @snapshot(5) { on E -> C }
`,
			wantErr: "unknown directive @snapshot",
		},
	}
	for name, tc := range cases {
		_, err := sdl.Parse(tc.src)
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: want error containing %q, got %v", name, tc.wantErr, err)
		}
	}
}
