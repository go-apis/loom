package loom

import (
	"strconv"
	"strings"
	"testing"
)

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
	t.Setenv("K_SERVICE", "")
	t.Setenv("LOOM_GENERATION", "")
	// the revision's sequence is not a generation: Cloud Run does not
	// number revisions in deploy order
	if g := generationFrom(0, ""); g.n != 0 || g.lookup || g.revision != "mesh-api-00021-j42" {
		t.Errorf("from K_REVISION alone: %+v", g)
	}
	if g := generationFrom(1761955200, ""); g.n != 1761955200 || g.source != genFromConfig || g.revision != "mesh-api-00021-j42" {
		t.Errorf("explicit: %+v", g)
	}
	if g := generationFrom(-1, ""); g.n != 0 || g.lookup {
		t.Errorf("turned off: %+v", g)
	}

	t.Setenv("LOOM_GENERATION", "1793460000")
	if g := generationFrom(0, ""); g.n != 1793460000 || g.source != genFromEnv {
		t.Errorf("from LOOM_GENERATION: %+v", g)
	}
	if g := generationFrom(1761955200, ""); g.n != 1761955200 || g.source != genFromConfig {
		t.Errorf("Config.Generation over LOOM_GENERATION: %+v", g)
	}
	if g := generationFrom(-1, ""); g.n != 0 || g.lookup {
		t.Errorf("Config turns it off: %+v", g)
	}
	for _, bad := range []string{"abc", "0", "17e8", "1.5"} {
		t.Setenv("LOOM_GENERATION", bad)
		if g := generationFrom(0, ""); g.n != 0 || g.lookup || g.note == "" {
			t.Errorf("LOOM_GENERATION=%q: %+v", bad, g)
		}
	}
	t.Setenv("LOOM_GENERATION", "-1")
	t.Setenv("K_SERVICE", "mesh-api")
	if g := generationFrom(0, ""); g.n != 0 || g.lookup {
		t.Errorf("LOOM_GENERATION turns it off: %+v", g)
	}

	// on Cloud Run with no explicit generation: the revision's createTime
	t.Setenv("LOOM_GENERATION", "")
	if g := generationFrom(0, ""); g.n != 0 || !g.lookup {
		t.Errorf("on Cloud Run: %+v", g)
	}
	t.Setenv("K_REVISION", "")
	if g := generationFrom(0, ""); g.n != 0 || g.lookup || g.revision != "" {
		t.Errorf("unset: %+v", g)
	}
}

func TestLeaseName(t *testing.T) {
	g := generation{n: 1793460000, revision: "mesh-api-00021-j42"}
	for _, yielding := range []bool{false, true} {
		got, y, ok := parseLeaseName(leaseName(g, yielding))
		if !ok || y != yielding || got != (leaseGen{n: g.n, revision: g.revision}) {
			t.Errorf("round trip (yielding %v): %+v %v %v", yielding, got, y, ok)
		}
	}
	if got := leaseName(g, true); got != "loom lease g1793460000 yielding mesh-api-00021-j42" {
		t.Errorf("leaseName = %q", got)
	}
	if got, _, ok := parseLeaseName(leaseName(generation{n: 7}, false)); !ok || got.n != 7 || got.revision != "" || got.legacy {
		t.Errorf("no revision: %+v %v", got, ok)
	}
	// loom v0.85.0's lease: a bare revision sequence
	if got, y, ok := parseLeaseName("loom lease 380 latchkey-00380-xih"); !ok || !got.legacy || got.n != 380 || y || got.revision != "latchkey-00380-xih" {
		t.Errorf("a v0.85.0 lease: %+v %v %v", got, y, ok)
	}
	if got, y, ok := parseLeaseName("loom lease 380 yielding latchkey-00380-xih"); !ok || !got.legacy || !y {
		t.Errorf("a v0.85.0 lease yielding: %+v %v %v", got, y, ok)
	}
	for _, app := range []string{"", "mesh-api-00020-abc", "loom lease", "loom lease x", "loom lease 0 r",
		"loom lease g0 r", "loom lease g r", "loom lease g-5 r", "loom lease g+5 r", "loom lease gg5 r", "psql"} {
		if _, _, ok := parseLeaseName(app); ok {
			t.Errorf("parseLeaseName(%q) took it for a lease", app)
		}
	}
}

// v085ParseLeaseName and v085NewerAsking are loom v0.85.0's readings of a
// lease name and a lease-channel payload, verbatim but for the receiver:
// the instances of the previous build in a fleet that rolls onto this one.
func v085ParseLeaseName(app string) (n int64, ok bool) {
	f := strings.Fields(app)
	if len(f) < 3 || f[0] != "loom" || f[1] != "lease" {
		return 0, false
	}
	n, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func v085NewerAsking(service string, own int64, payload string) bool {
	svc, n, ok := strings.Cut(payload, " ")
	if !ok || svc != service {
		return false
	}
	gen, err := strconv.ParseInt(n, 10, 64)
	return err == nil && gen > own
}

// TestAMixedFleet is a rollout from loom v0.85.0: its instances order by
// the revision sequence, this build by deploy generation.
func TestAMixedFleet(t *testing.T) {
	mine := generation{n: 1793460000, revision: "latchkey-00355-7rg"}
	v085 := leaseGen{n: 380, legacy: true, revision: "latchkey-00380-xih"}

	// a v0.85.0 follower cannot read this build's lease, so it never
	// takes it, whatever its revision number
	if _, ok := v085ParseLeaseName(leaseName(mine, false)); ok {
		t.Error("v0.85.0 reads this build's lease name")
	}
	// this build takes the lease from a v0.85.0 leader, of any revision
	// number, and asks it in terms it yields to
	for _, n := range []int64{1, 380, 99999} {
		theirs := leaseGen{n: n, legacy: true}
		if !outranks(mine, theirs) {
			t.Errorf("a deploy generation does not outrank v0.85.0's revision %d", n)
		}
		if p := askPayload("latchkey", mine, theirs); !v085NewerAsking("latchkey", n, p) {
			t.Errorf("a v0.85.0 leader of revision %d does not yield to %q", n, p)
		}
	}
	// even a small explicit generation asks a v0.85.0 leader in terms it
	// yields to
	if p := askPayload("latchkey", generation{n: 3}, v085); !v085NewerAsking("latchkey", 380, p) {
		t.Errorf("a v0.85.0 leader does not yield to %q", p)
	}
	// a v0.85.0 follower's request (a bare revision sequence, however
	// large) never moves this build's lease
	c := &Client{reg: &Registry{Service: "latchkey"}, gen: mine}
	for _, p := range []string{"latchkey 381", "latchkey 99999", "latchkey 9999999999"} {
		if _, ok := c.newerAsking(p); ok {
			t.Errorf("a v0.85.0 request %q moved the lease", p)
		}
	}
	// a leader of this build is asked with a marked generation, which
	// v0.85.0 never sends
	if p := askPayload("latchkey", mine, leaseGen{n: 1793450000}); p != "latchkey g1793460000" {
		t.Errorf("askPayload = %q", p)
	}
	// with no generation, nothing is taken
	if outranks(generation{}, v085) {
		t.Error("no generation outranks a v0.85.0 leader")
	}
}

// TestOutranks: like compares with like, and the revision's number plays
// no part, so a revision Cloud Run numbered lower but deployed later
// still takes the lease.
func TestOutranks(t *testing.T) {
	older := generation{n: 1793440000, revision: "latchkey-00380-xih"} // deployed 18:55
	newer := generation{n: 1793453000, revision: "latchkey-00355-7rg"} // deployed 22:41
	held := func(g generation) leaseGen {
		got, _, _ := parseLeaseName(leaseName(g, false))
		return got
	}
	if !outranks(newer, held(older)) {
		t.Error("the later deploy, with the lower revision number, does not take the lease")
	}
	if outranks(older, held(newer)) {
		t.Error("the earlier deploy, with the higher revision number, takes the lease")
	}
	if outranks(newer, held(newer)) {
		t.Error("an equal generation takes the lease")
	}
	twin := generation{n: newer.n, revision: "latchkey-00356-abc"} // an env-only redeploy keeping LOOM_GENERATION
	if outranks(twin, held(newer)) || outranks(newer, held(twin)) {
		t.Error("an equal generation on another revision takes the lease")
	}
}

func TestNewerAsking(t *testing.T) {
	c := &Client{reg: &Registry{Service: "mesh"}, gen: generation{n: 21}}
	for _, c2 := range []struct {
		payload string
		ok      bool
	}{
		{"mesh g22", true},
		{"mesh g21", false},
		{"mesh g20", false},
		{"mesh 22", false}, // v0.85.0's revision sequence
		{"other g99", false},
		{"mesh", false},
		{"mesh x", false},
		{"mesh g+22", false},
		{"mesh gx", false},
	} {
		if _, ok := c.newerAsking(c2.payload); ok != c2.ok {
			t.Errorf("newerAsking(%q) = %v", c2.payload, ok)
		}
	}
}
