package sdl

import (
	"strings"
	"testing"
)

func TestSeriesRetain(t *testing.T) {
	src := `service metrics

series Sample @time(at) @dim(route) @retain(90d) {
  route: string!
  at:    timestamp!
  ms:    float!
}

series Weekly @time(at) @dim(k) @retain("2w") {
  k:  string!
  at: timestamp!
}

series Forever @time(at) @dim(k) {
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
	got := map[string]int{}
	for _, sr := range s.Series {
		got[sr.Name] = sr.RetainDays
	}
	if got["Sample"] != 90 || got["Weekly"] != 14 || got["Forever"] != 0 {
		t.Fatalf("retain days = %v", got)
	}

	for name, bad := range map[string]string{
		"two args":  `series X @time(at) @dim(k) @retain(1, 2) { k: string! at: timestamp! }`,
		"bad unit":  `series X @time(at) @dim(k) @retain(90x) { k: string! at: timestamp! }`,
		"zero":      `series X @time(at) @dim(k) @retain(0d) { k: string! at: timestamp! }`,
		"aggregate": `aggregate X @retain(90d) { state { a: string } command Do {} -> Done event Done { a: string } }`,
	} {
		if _, err := Parse("service m\n\n" + bad); err == nil || !strings.Contains(err.Error(), "retain") {
			t.Errorf("%s: err = %v, want a @retain error", name, err)
		}
	}
}

func TestLexerDurationToken(t *testing.T) {
	toks, err := lex(`@snapshot(5) @retain(90d)`)
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for _, tk := range toks {
		if tk.kind == tNumber {
			texts = append(texts, tk.text)
		}
	}
	if len(texts) != 2 || texts[0] != "5" || texts[1] != "90d" {
		t.Fatalf("number tokens = %v", texts)
	}
}
