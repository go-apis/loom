package sdl

import (
	"slices"
	"strings"
	"testing"
)

// @compress and @rollup (ADR 0011): parsed onto the series, validated
// against @retain and the rollup units, refused anywhere else.
func TestSeriesCompressAndRollup(t *testing.T) {
	src := `service metrics

series Price @time(at) @dim(product) @retain(400d) @compress(30d) @rollup(hour, day) {
  product: string!
  at:      timestamp!
  cents:   int!
}

series Plain @time(at) @dim(k) {
  k:  string!
  at: timestamp!
}
`
	s, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, sr := range s.Series {
		switch sr.Name {
		case "Price":
			if sr.CompressDays != 30 || !slices.Equal(sr.Rollups, []string{"hour", "day"}) {
				t.Fatalf("Price: compress %d rollups %v", sr.CompressDays, sr.Rollups)
			}
		case "Plain":
			if sr.CompressDays != 0 || len(sr.Rollups) != 0 {
				t.Fatalf("Plain: compress %d rollups %v", sr.CompressDays, sr.Rollups)
			}
		}
	}

	for name, c := range map[string]struct{ src, want string }{
		"compress two args":     {`series X @time(at) @dim(k) @compress(1d, 2d) { k: string! at: timestamp! }`, "compress"},
		"compress bad":          {`series X @time(at) @dim(k) @compress(7x) { k: string! at: timestamp! }`, "compress"},
		"compress on aggregate": {`aggregate X @compress(7d) { state { a: string } command Do {} -> Done event Done { a: string } }`, "compress"},
	} {
		if _, err := Parse("service m\n\n" + c.src); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want a @%s error", name, err, c.want)
		}
	}

	for name, c := range map[string]struct{ src, want string }{
		"compress past retain": {`series X @time(at) @dim(k) @retain(30d) @compress(30d) { k: string! at: timestamp! }`, "under @retain"},
		"rollup unit":          {`series X @time(at) @dim(k) @rollup(month) { k: string! at: timestamp! }`, "not a rollup unit"},
		"rollup twice":         {`series X @time(at) @dim(k) @rollup(day, day) { k: string! at: timestamp! }`, "twice"},
	} {
		s, err := Parse("service m\n\n" + c.src)
		if err == nil {
			err = s.Validate()
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: validate = %v, want %q", name, err, c.want)
		}
	}
}
