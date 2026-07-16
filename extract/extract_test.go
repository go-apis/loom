package extract_test

import (
	"bytes"
	"testing"

	"github.com/go-apis/loom/extract"
)

// TestExtractExampleUsers runs extraction against the in-repo example
// service, covering the full pipeline: registry discovery, constructor
// resolution, handle scanning, tags, and payload derivation.
func TestExtractExampleUsers(t *testing.T) {
	res, err := extract.Service("../testdata/users", "users")
	if err != nil {
		t.Fatal(err)
	}
	s := res.Schema
	if len(s.Aggregates) == 0 || len(s.Events) == 0 {
		t.Fatalf("empty extraction: %d aggregates, %d events", len(s.Aggregates), len(s.Events))
	}

	first, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// determinism: a second extraction of unchanged code must be
	// byte-identical, so schema drift always shows as a clean git diff.
	res2, err := extract.Service("../testdata/users", "users")
	if err != nil {
		t.Fatal(err)
	}
	second, err := res2.Schema.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("extraction output is not deterministic")
	}
}
