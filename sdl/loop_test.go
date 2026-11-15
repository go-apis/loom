package sdl_test

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

func TestParseAggregateLoop(t *testing.T) {
	s, err := sdl.Parse(`
service orders

aggregate Order @loop {
  state {
    status: string
  }
  command PlaceOrder {
    status: string!
  } -> OrderPlaced
  event OrderPlaced {
    status: string!
  }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Aggregates) != 1 || !s.Aggregates[0].Loop {
		t.Fatalf("aggregate @loop misparsed: %+v", s.Aggregates)
	}
}

func TestParseRecordLoopRejected(t *testing.T) {
	_, err := sdl.ParseFiles([]sdl.File{
		{Path: "schema/ledger.loom", Src: `service orders

record Ledger @loop {
  state {
    total_cents: int
  }
  command PostLedger {
    amount_cents: int!
  }
}
`},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "schema/ledger.loom:3:") || !strings.Contains(err.Error(), "unknown directive @loop") {
		t.Fatalf("want an error at schema/ledger.loom:3: naming @loop, got %v", err)
	}
}
