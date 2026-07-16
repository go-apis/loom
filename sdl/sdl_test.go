package sdl_test

import (
	"bytes"
	"testing"

	"github.com/go-apis/loom/extract"
	"github.com/go-apis/loom/schema"
	"github.com/go-apis/loom/sdl"
)

// TestRoundTrip proves the SDL is a lossless surface for the compiled
// schema: extract the example service, emit .loom, parse it back, and
// compare against the original with extraction-only Go metadata stripped.
func TestRoundTrip(t *testing.T) {
	res, err := extract.Service("../testdata/users", "users")
	if err != nil {
		t.Fatal(err)
	}
	stripGo(res.Schema)

	src := sdl.Emit(res.Schema)
	parsed, err := sdl.Parse(string(src))
	if err != nil {
		t.Fatalf("parse emitted SDL: %v\n%s", err, src)
	}

	want, err := res.Schema.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := parsed.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("round trip mismatch\n--- original ---\n%s\n--- round-tripped ---\n%s\n--- sdl ---\n%s", want, got, src)
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]string{
		"missing service": "aggregate X {}",
		"undeclared type": "service s\naggregate X {\n  command C { a: Missing }\n}",
		"bad token":       "service s\n$",
	}
	for name, src := range cases {
		if _, err := sdl.Parse(src); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func stripGo(s *schema.Schema) {
	for _, a := range s.Aggregates {
		a.Go = ""
		for _, c := range a.Commands {
			c.Go = ""
		}
	}
	for _, e := range s.Events {
		e.Go = ""
	}
	for _, h := range s.Handlers {
		h.Go = ""
		for _, c := range h.Commands {
			c.Go = ""
		}
	}
	for _, ty := range s.Types {
		ty.Go = ""
	}
}
