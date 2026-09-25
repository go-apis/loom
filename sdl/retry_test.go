package sdl_test

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

const retrySrc = `
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
`

// TestRetryDirective proves `@retry(max, min..max)` on a process parses
// into the schema's durable retry policy, alongside @from.
func TestRetryDirective(t *testing.T) {
	s, err := sdl.Parse(retrySrc + `
process p @retry(20, 5s..5m) @from(origin) {
  on E -> C
}
process q {
  on E -> C
}
`)
	if err != nil {
		t.Fatal(err)
	}
	r := s.Processes[0].Retry
	if r == nil || r.Max != 20 || r.Min != "5s" || r.MaxBackoff != "5m" {
		t.Fatalf("@retry misparsed: %+v", r)
	}
	if s.Processes[0].From != "origin" {
		t.Fatalf("@from lost beside @retry: %q", s.Processes[0].From)
	}
	if s.Processes[1].Retry != nil {
		t.Fatalf("undeclared process grew a retry policy: %+v", s.Processes[1].Retry)
	}
}

func TestRetryDirectiveRejects(t *testing.T) {
	for _, tc := range []struct{ decl, want string }{
		{"process p @retry(5) {\n on E -> C\n}", "backoff range"},
		{"process p @retry(5, 5s) {\n on E -> C\n}", "backoff range"},
		{"process p @retry(x, 1s..2s) {\n on E -> C\n}", "backoff range"},
		{"process p @retry(0, 1s..2s) {\n on E -> C\n}", "at least one attempt"},
		{"process p @retry(5, 5m..5s) {\n on E -> C\n}", "exceeds maximum"},
		{"process p @retry(5, 5..10) {\n on E -> C\n}", "bad minimum backoff"},
		{"policy p @retry(5, 1s..2s) {\n on E -> C\n}", "cannot declare @retry"},
	} {
		_, err := sdl.Parse(retrySrc + tc.decl)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.decl, err, tc.want)
		}
	}
}
