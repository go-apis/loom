package loom

import "testing"

func TestRevisionSequence(t *testing.T) {
	for _, c := range []struct {
		in string
		n  int64
		ok bool
	}{
		{"mesh-api-00021-j42", 21, true},
		{"runsheet-01158-req", 1158, true},
		{"latchkey-00300-x", 300, true},
		{"mesh-api-canary", 0, false},
		{"mesh-api", 0, false},
		{"mesh-00000-abc", 0, false},
		{"mesh-+12-abc", 0, false},
		{"mesh-00012-", 0, false},
		{"", 0, false},
	} {
		n, ok := RevisionSequence(c.in)
		if n != c.n || ok != c.ok {
			t.Errorf("RevisionSequence(%q) = %d %v, want %d %v", c.in, n, ok, c.n, c.ok)
		}
	}
}

func TestGenerationFrom(t *testing.T) {
	t.Setenv("K_REVISION", "mesh-api-00021-j42")
	if g := generationFrom(0, ""); g.n != 21 || g.revision != "mesh-api-00021-j42" {
		t.Errorf("from K_REVISION: %+v", g)
	}
	if g := generationFrom(1761955200, ""); g.n != 1761955200 || g.revision != "mesh-api-00021-j42" {
		t.Errorf("explicit: %+v", g)
	}
	if g := generationFrom(-1, ""); g.n != 0 {
		t.Errorf("turned off: %+v", g)
	}
	if g := generationFrom(0, "mesh-api-canary"); g.n != 0 || g.revision != "mesh-api-canary" {
		t.Errorf("a custom revision name: %+v", g)
	}
	t.Setenv("K_REVISION", "")
	if g := generationFrom(0, ""); g.n != 0 || g.revision != "" {
		t.Errorf("unset: %+v", g)
	}
}

func TestLeaseName(t *testing.T) {
	g := generation{n: 21, revision: "mesh-api-00021-j42"}
	for _, yielding := range []bool{false, true} {
		got, y, ok := parseLeaseName(leaseName(g, yielding))
		if !ok || y != yielding || got != g {
			t.Errorf("round trip (yielding %v): %+v %v %v", yielding, got, y, ok)
		}
	}
	if got, _, ok := parseLeaseName(leaseName(generation{n: 7}, false)); !ok || got.n != 7 || got.revision != "" {
		t.Errorf("no revision: %+v %v", got, ok)
	}
	for _, app := range []string{"", "mesh-api-00020-abc", "loom lease", "loom lease x", "loom lease 0 r", "psql"} {
		if _, _, ok := parseLeaseName(app); ok {
			t.Errorf("parseLeaseName(%q) took it for a lease", app)
		}
	}
}

func TestNewerAsking(t *testing.T) {
	c := &Client{reg: &Registry{Service: "mesh"}, gen: generation{n: 21}}
	for _, c2 := range []struct {
		payload string
		ok      bool
	}{
		{"mesh 22", true},
		{"mesh 21", false},
		{"mesh 20", false},
		{"other 99", false},
		{"mesh", false},
		{"mesh x", false},
	} {
		if _, ok := c.newerAsking(c2.payload); ok != c2.ok {
			t.Errorf("newerAsking(%q) = %v", c2.payload, ok)
		}
	}
}
