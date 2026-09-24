package schema

import (
	"strings"
	"testing"
)

func obj(field string) *Payload {
	return &Payload{Type: "object", Properties: map[string]*Payload{field: {Type: "string"}}}
}

// base is the smallest valid schema: one aggregate, one command, the
// event it emits. Each case below adds a reactor to it.
func base() *Schema {
	return &Schema{
		Loom:    Version,
		Service: "s",
		Aggregates: []*Aggregate{{
			Name:     "A",
			State:    obj("x"),
			Commands: []*Command{{Name: "C", Emits: []string{"E"}}},
		}},
		Events: []*Event{{Name: "E", Payload: obj("x")}},
	}
}

// TestStartPositionIsProcessOnly: @from says where a runner that has
// never checkpointed begins. A policy runs inside the producing
// transaction — there is no "before it existed" to start after — so
// declaring one is a schema error, however the schema was authored.
func TestStartPositionIsProcessOnly(t *testing.T) {
	s := base()
	s.Policies = []*Reactor{{Name: "p", From: FromHead, Subscriptions: []*Subscription{{Event: "E", Dispatches: []string{"C"}}}}}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "has no start position") {
		t.Fatalf("policy @from(head) should be rejected, got %v", err)
	}
}

func TestStartPositionValues(t *testing.T) {
	for _, from := range []string{"", FromHead, FromOrigin} {
		s := base()
		s.Processes = []*Reactor{{Name: "p", From: from, Subscriptions: []*Subscription{{Event: "E", Dispatches: []string{"C"}}}}}
		if err := s.Validate(); err != nil {
			t.Fatalf("process @from(%q): %v", from, err)
		}
	}
	s := base()
	s.Processes = []*Reactor{{Name: "p", From: "tuesday", Subscriptions: []*Subscription{{Event: "E", Dispatches: []string{"C"}}}}}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "is not a start position") {
		t.Fatalf("process @from(tuesday) should be rejected, got %v", err)
	}
}
