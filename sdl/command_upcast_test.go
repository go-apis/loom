package sdl_test

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

// TestCommandUpcast: a command takes @v, and `upcast X @from(n)` — the
// event syntax — lifts a command when X names one, wherever the upcast
// sits relative to the declaration. An event's upcast is unchanged.
func TestCommandUpcast(t *testing.T) {
	s, err := sdl.Parse(`
service s
upcast Cancel @from(1, 2)
aggregate A {
  state { x: string }
  command Cancel @v(3) { reason: string } -> E
  event E @v(2) { x: string }
}
record R {
  state { n: int }
  command Bump @v(2) -> E
}
upcast E @from(1)
upcast Bump @from(1)
`)
	if err != nil {
		t.Fatal(err)
	}
	_, cancel := s.FindCommand("Cancel")
	if cancel.Version != 3 || len(cancel.Upcasts) != 2 || cancel.Upcasts[0] != 1 || cancel.Upcasts[1] != 2 {
		t.Fatalf("Cancel: version %d upcasts %v", cancel.Version, cancel.Upcasts)
	}
	_, bump := s.FindRecordCommand("Bump")
	if bump.Version != 2 || len(bump.Upcasts) != 1 || bump.Upcasts[0] != 1 {
		t.Fatalf("Bump: version %d upcasts %v", bump.Version, bump.Upcasts)
	}
	if len(s.Events) != 1 || s.Events[0].Name != "E" || len(s.Events[0].Upcasts) != 1 {
		t.Fatalf("events: %+v", s.Events)
	}
}

func TestCommandUpcastRefusals(t *testing.T) {
	for name, tc := range map[string]struct{ src, want string }{
		"hop at or above the command's version": {`
service s
aggregate A {
  state { x: string }
  command C { y: string } -> E
  event E { x: string }
}
upcast C @from(1)
`, "command C: upcast @from(1) must name a version below the command's @v(1)"},
		"coverage gap": {`
service s
aggregate A {
  state { x: string }
  command C @v(3) -> E
  event E { x: string }
}
upcast C @from(1)
`, "command C: upcast coverage has a gap — @from(2) is missing"},
		"a name that is a command and an event": {`
service s
aggregate A {
  state { x: string }
  command C @v(2) -> C
  event C { x: string }
}
upcast C @from(1)
`, "C is both a command and an event"},
		"bad @v": {`
service s
aggregate A {
  state { x: string }
  command C @v(0) -> E
  event E { x: string }
}
`, "command C: @v wants a positive version"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sdl.Parse(tc.src)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}
