package gen

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

// TestAccessSaysWhoMayRunWhat: every command and generated read is a
// row under the name the gateway serves, with its roles, its rules read
// aloud, and whether it is planned first — in a fixed order, so the
// document changes only when the access does.
func TestAccessSaysWhoMayRunWhat(t *testing.T) {
	const src = `service shop
aggregate Parcel @table @allow(kind: admin) @allow(kind: courier, depot_id: caller.depot) {
  state { depot_id: uuid  value: int @allow(kind: admin) @allow(kind: auditor) }
  command Zap @plan @role(owner) @allow(tier: gold, kind: courier, depot_id: caller.depot) -> Zapped
  command Add -> Zapped
  event Zapped
}
entity Row @role(owner, auditor) { n: int }
record Tally { state { n: int } }
`
	s, err := sdl.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.Sort()
	out, err := Access(s)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(out)
	for _, want := range []string{
		"# Who may run what: shop\n",
		"| `add` | Parcel | *any* | *any caller* | at once |\n| `zap` | Parcel | owner | `kind` is `courier` and `tier` is `gold` and its `depot_id` is the caller's `depot` | planned first |\n",
		"| `parcel` | Parcel | *any* | `kind` is `admin`; **or** `kind` is `courier` and its `depot_id` is the caller's `depot` |\n| `parcels` | Parcel |",
		"| `row` | Row | owner, auditor | *any caller* |\n| `rows` | Row | owner, auditor | *any caller* |\n",
		"| `tally` | Tally | *any* | *any caller* |\n| `tallys` | Tally |",
		"## Fields shown to some callers\n",
		"| Parcel | `value` | `kind` is `admin`; **or** `kind` is `auditor` |\n",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("the access table lacks %q:\n%s", want, doc)
		}
	}
	again, _ := Access(s)
	if string(again) != doc {
		t.Error("the access table is not the same twice")
	}
}
