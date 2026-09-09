package schema

import "testing"

func TestParseRetain(t *testing.T) {
	good := map[string]int{"90d": 90, "2w": 14, "36h": 2, "24h": 1, "7": 7, " 1D ": 1}
	for in, want := range good {
		got, err := ParseRetain(in)
		if err != nil || got != want {
			t.Errorf("ParseRetain(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0d", "-1d", "d", "90x", "1.5d", "abc"} {
		if _, err := ParseRetain(in); err == nil {
			t.Errorf("ParseRetain(%q): expected error", in)
		}
	}
}
