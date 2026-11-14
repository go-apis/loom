package sdl

import (
	"strings"
	"testing"
	"time"
)

// TestScheduleParses: the syntax, the interval units, and what a
// schedule may not be.
func TestScheduleParses(t *testing.T) {
	const agg = `service s
aggregate Watch {
  state { n: int }
  command Tick -> Ticked
  command Poke { why: string! } -> Ticked
  event Ticked { n: int! }
}
`
	s, err := Parse(agg + `/// Looks every five minutes.
schedule look every 5m -> Tick @in(platform)
schedule daily every 1d -> Tick @in(platform) @id("11111111-2222-4333-8444-555555555555")
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.Sort()
	daily, look := s.Schedules[0], s.Schedules[1]
	if look.Name != "look" || look.Command != "Tick" || look.Namespace != "platform" || look.ID != "" || look.Doc != "Looks every five minutes." {
		t.Errorf("look = %+v", look)
	}
	if d, err := look.Interval(); err != nil || d != 5*time.Minute {
		t.Errorf("look's interval = %v, %v", d, err)
	}
	if d, err := daily.Interval(); err != nil || d != 24*time.Hour || daily.ID != "11111111-2222-4333-8444-555555555555" {
		t.Errorf("daily = %+v, interval %v, %v", daily, d, err)
	}

	for src, want := range map[string]string{
		`schedule x every 5m -> Tick`:                                            "names no namespace",
		`schedule x every soon -> Tick @in(p)`:                                   "every wants an interval",
		`schedule x every 5parsecs -> Tick @in(p)`:                               "is not an interval",
		`schedule x every 5m -> Nothing @in(p)`:                                  "which no aggregate or record declares",
		`schedule x every 5m -> Poke @in(p)`:                                     "a scheduled command carries no payload",
		`schedule x every 5m -> Tick @in(p) @id("not-a-uuid")`:                   "is not a uuid",
		`schedule x every 5m -> Tick @in(p) @often`:                              "unknown or malformed directive @often",
		"schedule x every 5m -> Tick @in(p)\nschedule x every 1h -> Tick @in(p)": "declared twice",
		`schedule x 5m -> Tick @in(p)`:                                           "expected `every <interval>`",
	} {
		s, err := Parse(agg + src)
		if err == nil {
			err = s.Validate()
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want one saying %q", src, err, want)
		}
	}
}
