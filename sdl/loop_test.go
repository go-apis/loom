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

const deadlineSchema = `
service orders

enum Stage { open running done }

aggregate Order @loop {
  state {
    status: string
    stage: Stage
    count: int
  }
  command PlaceOrder {
    status: string!
  } -> OrderPlaced
  event OrderPlaced {
    status: string!
  }
  %s
}
`

func TestParseADeadlineOnALoop(t *testing.T) {
	s, err := sdl.Parse(strings.Replace(deadlineSchema, "%s", `deadline stale after 30m while status in ("running", "waiting")
  deadline stuck after 1d while stage in (running)`, 1))
	if err != nil {
		t.Fatal(err)
	}
	dls := s.Aggregates[0].Deadlines
	if len(dls) != 2 {
		t.Fatalf("parsed %d deadlines, want 2: %+v", len(dls), dls)
	}
	if d := dls[0]; d.Name != "stale" || d.After != "30m" || d.Field != "status" || strings.Join(d.Values, ",") != "running,waiting" {
		t.Errorf("first deadline is %+v", d)
	}
	if d := dls[1]; d.Name != "stuck" || d.After != "1d" || d.Field != "stage" || strings.Join(d.Values, ",") != "running" {
		t.Errorf("second deadline is %+v", d)
	}
	if got, err := dls[1].Duration(); err != nil || got.Hours() != 24 {
		t.Errorf("1d reads as %s (%v), want 24h", got, err)
	}
}

func TestParseADeadlineRefused(t *testing.T) {
	for name, c := range map[string]struct{ src, want string }{
		"on an aggregate with no @loop": {strings.Replace(strings.Replace(deadlineSchema, "@loop ", "", 1), "%s", `deadline stale after 30m while status in ("running")`, 1), "@loop"},
		"naming a missing field":        {strings.Replace(deadlineSchema, "%s", `deadline stale after 30m while phase in ("running")`, 1), "no field phase"},
		"on an int field":               {strings.Replace(deadlineSchema, "%s", `deadline stale after 30m while count in ("1")`, 1), "not a string or an enum"},
		"with no values":                {strings.Replace(deadlineSchema, "%s", `deadline stale after 30m while status in ()`, 1), "at least one value"},
		"with no duration":              {strings.Replace(deadlineSchema, "%s", `deadline stale after soon while status in ("running")`, 1), "duration"},
		"with no while":                 {strings.Replace(deadlineSchema, "%s", `deadline stale after 30m status in ("running")`, 1), "while"},
	} {
		_, err := sdl.Parse(c.src)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("a deadline %s: error %v, want one naming %q", name, err, c.want)
		}
	}
}
