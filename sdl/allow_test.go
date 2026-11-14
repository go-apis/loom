package sdl

import (
	"reflect"
	"strings"
	"testing"

	"github.com/go-apis/loom/schema"
)

// TestAllowRulesParse: @allow repeats, one rule each; `attr: value` is
// a caller pair and `field: caller.attr` an own pair. It is taken on
// aggregates, records, entities and commands and nowhere else, and an
// own field must be one the gateway can read.
func TestAllowRulesParse(t *testing.T) {
	s, err := Parse(`service shop
aggregate Parcel @allow(kind: admin) @allow(kind: courier, tier: "gold", depot_id: caller.depot) {
  state { depot_id: uuid }
  command Register @role(owner) @allow(kind: courier, depot_id: caller.depot) { depot_id: uuid! } -> Registered
  command Touch -> Registered
  event Registered { depot_id: uuid! }
}
entity ParcelRow @allow(depot_id: caller.depot) { depot_id: uuid }
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	agg := s.Aggregates[0]
	want := []*schema.Rule{
		{Caller: map[string]string{"kind": "admin"}},
		{Caller: map[string]string{"kind": "courier", "tier": "gold"}, Own: map[string]string{"depot_id": "depot"}},
	}
	if !reflect.DeepEqual(agg.ReadAllow, want) {
		t.Errorf("aggregate rules = %+v", agg.ReadAllow)
	}
	for _, c := range agg.Commands {
		switch c.Name {
		case "Register":
			if len(c.Allow) != 1 || c.Allow[0].Own["depot_id"] != "depot" || len(c.Roles) != 1 {
				t.Errorf("Register: allow %+v roles %v", c.Allow, c.Roles)
			}
		case "Touch":
			if c.Allow != nil {
				t.Errorf("Touch took the rules of the command before it: %+v", c.Allow)
			}
		}
	}
	if got := s.Entities[0].ReadAllow; len(got) != 1 || got[0].Own["depot_id"] != "depot" {
		t.Errorf("entity rules = %+v", got)
	}

	for src, wantErr := range map[string]string{
		`service s
aggregate A { state { x: string @allow(kind: admin) } }`: "unknown directive @allow",
		`service s
aggregate A @allow(kind) { state { x: string } }`: `expected ":"`,
		`service s
aggregate A @allow(kind: admin, kind: other) { state { x: string } }`: "twice",
		`service s
aggregate A @allow(owner_id: caller.me) { state { x: string } }`: "neither in its state nor in its payload",
	} {
		s, err := Parse(src)
		if err == nil {
			err = s.Validate()
		}
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("%q: err = %v, want one saying %q", src, err, wantErr)
		}
	}
}
