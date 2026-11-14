package sdl

import "testing"

// TestDocCommentsLandOnWhatTheySitAbove: a `///` block directly above a
// declaration or a field is its doc, lines joined into prose and an
// empty `///` line starting a paragraph. A plain `//` comment is a note
// and is never published: not on its own, not when it sits between the
// doc and the declaration. A blank line detaches a doc, and a comment
// after a token on its own line documents nothing.
func TestDocCommentsLandOnWhatTheySitAbove(t *testing.T) {
	s, err := Parse(`service shop

/// How soon it ships.
enum Priority { standard rush }

/// A customer's order,
/// from placed to shipped.
///
/// Second paragraph.
aggregate Order {
  state {
    /// What it adds up to.
    total_cents: int
    // a note for the schema's reader
    currency: string
    status: string // trailing, about nothing
    /// Detached by the blank line below.

    reason: string
  }

  /// Place an order.
  command PlaceOrder {
    /// ISO 4217.
    currency: string!
  } -> OrderPlaced

  // internal: gated at the gateway
  command ShipOrder -> OrderShipped

  /// Lost: the note below ends it.
  // a note
  command CancelOrder -> OrderCancelled

  event OrderPlaced { currency: string! }
  event OrderShipped
  event OrderCancelled
}

/// One line.
type Item {
  /// Stock-keeping unit.
  sku: string!
}

/// The list page's row.
entity OrderRow {
  /// Shown in the list.
  currency: string
}

/// One per account.
record Balance {
  state { cents: int }
  /// Add to it.
  command Credit { cents: int! } -> Credited
  event Credited { cents: int! }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	agg := s.Aggregates[0]
	cmd := map[string]string{}
	for _, c := range agg.Commands {
		cmd[c.Name] = c.Doc
	}
	place := agg.Commands[0]
	for _, c := range agg.Commands {
		if c.Name == "PlaceOrder" {
			place = c
		}
	}
	rec := s.Records[0]
	for _, c := range []struct{ what, got, want string }{
		{"enum", s.Enums[0].Doc, "How soon it ships."},
		{"aggregate", agg.Doc, "A customer's order, from placed to shipped.\n\nSecond paragraph."},
		{"state field", agg.State.Properties["total_cents"].Doc, "What it adds up to."},
		{"field under a note", agg.State.Properties["currency"].Doc, ""},
		{"field after a trailing comment", agg.State.Properties["status"].Doc, ""},
		{"field under a detached doc", agg.State.Properties["reason"].Doc, ""},
		{"command", cmd["PlaceOrder"], "Place an order."},
		{"command field", place.Payload.Properties["currency"].Doc, "ISO 4217."},
		{"command under a note", cmd["ShipOrder"], ""},
		{"command under a doc then a note", cmd["CancelOrder"], ""},
		{"type", s.Types[0].Doc, "One line."},
		{"type field", s.Types[0].Payload.Properties["sku"].Doc, "Stock-keeping unit."},
		{"entity", s.Entities[0].Doc, "The list page's row."},
		{"entity field", s.Entities[0].State.Properties["currency"].Doc, "Shown in the list."},
		{"record", rec.Doc, "One per account."},
		{"record command", rec.Commands[0].Doc, "Add to it."},
	} {
		if c.got != c.want {
			t.Errorf("%s doc = %q, want %q", c.what, c.got, c.want)
		}
	}
}
