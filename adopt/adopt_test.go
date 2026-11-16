package adopt

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixture = "testdata/repo"

// TestAdoptFindsEachRule: one finding per rule in the fixture, at the
// right place, with the feature to use and the span it covers; tests,
// generated code, vendor, a ticker read once, a one-shot loom.After and a
// policy that is not a timer are left alone.
func TestAdoptFindsEachRule(t *testing.T) {
	got, err := Scan(fixture)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		rule, file    string
		line, lines   int
		featureHasAll string
	}{
		{"ticker-goroutine", "svc/sweep.go", 10, 12, "Client.Every"},
		{"self-rearming-timer", "svc/watch.go", 16, 3, "schedule"},
		{"timer-policy", "svc/schema/order.loom", 6, 4, "@loop"},
	}
	if len(got) != len(want) {
		t.Fatalf("found %d, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.Rule != w.rule || g.File != w.file || g.Line != w.line || g.Lines != w.lines || !strings.Contains(g.Feature, w.featureHasAll) {
			t.Errorf("finding %d is %+v, want %s at %s:%d over %d lines naming %s", i, g, w.rule, w.file, w.line, w.lines, w.featureHasAll)
		}
	}
	if !strings.Contains(got[1].What, "TickWatch") {
		t.Errorf("the re-arm finding says %q, want it to name TickWatch", got[1].What)
	}
}

// TestAdoptChangesNothingAndExitsZero: a scan writes nothing, and the
// report is advice: it names each rule's feature and says so.
func TestAdoptChangesNothingAndExitsZero(t *testing.T) {
	before := snapshot(t, fixture)
	got, err := Scan(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	Report(&b, got)
	if after := snapshot(t, fixture); !equal(before, after) {
		t.Error("a scan changed the fixture's files")
	}
	out := b.String()
	for _, r := range Rules {
		if !strings.Contains(out, r.Feature) || !strings.Contains(out, r.Since) {
			t.Errorf("the report does not name %s's feature and release", r.ID)
		}
	}
	if !strings.Contains(out, "advice") {
		t.Error("the report does not say it is advice")
	}
	var empty strings.Builder
	Report(&empty, nil)
	if !strings.Contains(empty.String(), "nothing found") {
		t.Errorf("an empty report reads %q", empty.String())
	}
}

// TestEveryRuleIsDocumented: every rule has its feature's entry in
// docs/features.md, by its id.
func TestEveryRuleIsDocumented(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "docs", "features.md"))
	if err != nil {
		t.Skip("docs/features.md is written in the next item")
	}
	for _, r := range Rules {
		if !bytes.Contains(doc, []byte("`"+r.ID+"`")) {
			t.Errorf("docs/features.md does not name adopt rule `%s`", r.ID)
		}
	}
}

func snapshot(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		files[p] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func equal(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !bytes.Equal(v, b[k]) {
			return false
		}
	}
	return true
}
