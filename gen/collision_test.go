package gen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

// A process, a policy and a @fold projection named after an aggregate of
// the same schema — `aggregate FleetStall` beside `process fleetStall`,
// the shape runsheet hand-patched for two weeks — get Impl fields with
// their kind appended (FleetStallProcess, OrgMembershipPolicy,
// SnapshotProjection), the calls through them follow, the Reactions and
// Folds interfaces keep the declared name, a reactor that collides with
// nothing keeps its plain field, and the generated package compiles (ADR
// 0006).
func TestReactorFieldNamedAfterAnAggregate(t *testing.T) {
	s, err := sdl.Parse(`
service s
aggregate FleetStall {
  state { x: string }
  command Open -> Opened
  event Opened { x: string }
}
aggregate OrgMembership {
  state { y: string }
  command Join -> Joined
  event Joined { y: string }
}
aggregate Snapshot {
  state { z: string }
  command Take -> Taken
  event Taken { z: string }
}
entity SnapshotView {
  z: string
}
process fleetStall {
  on Opened -> Join
}
policy orgMembership {
  on Joined -> Take
}
projection snapshot -> SnapshotView @fold {
  on Taken
}
process plain {
  on Taken -> Open
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := gen.Generate(s, gen.Config{Dir: dir, Package: "s", Module: "example.com/s", Layout: "folders"})
	if err != nil {
		t.Fatal(err)
	}
	// every declaration's interface and its binding method on Impl are in
	// its own file (ADR 0008); a reactor named like an aggregate binds
	// under its name with its kind appended
	read := func(name string) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, "loomgen", name))
		if err != nil {
			t.Fatalf("%v (wrote %v)", err, res.Written)
		}
		return strings.Join(strings.Fields(string(raw)), " ")
	}
	registry := read("loom_gen.go")
	for file, decls := range map[string][]string{
		"fleet_stall_gen.go":           {"type FleetStallHandlers interface", "func (impl *Impl) FleetStall(h FleetStallHandlers)"},
		"org_membership_gen.go":        {"type OrgMembershipHandlers interface", "func (impl *Impl) OrgMembership(h OrgMembershipHandlers)"},
		"snapshot_gen.go":              {"type SnapshotHandlers interface", "func (impl *Impl) Snapshot(h SnapshotHandlers)"},
		"fleet_stall_process_gen.go":   {"type FleetStallReactions interface", "func (impl *Impl) FleetStallProcess(h FleetStallReactions)", "reactions.OnOpened("},
		"org_membership_policy_gen.go": {"type OrgMembershipReactions interface", "func (impl *Impl) OrgMembershipPolicy(h OrgMembershipReactions)", "reactions.OnJoined("},
		"snapshot_projection_gen.go":   {"type SnapshotFolds interface", "func (impl *Impl) SnapshotProjection(h SnapshotFolds)", "folds.OnTaken("},
		"plain_process_gen.go":         {"type PlainReactions interface", "func (impl *Impl) Plain(h PlainReactions)", "reactions.OnTaken("},
	} {
		for _, d := range decls {
			if !strings.Contains(read(file), d) {
				t.Fatalf("%s has no %q", file, d)
			}
		}
	}
	for _, unwanted := range []string{"Reactions interface", "Folds interface", "FleetStall", "OrgMembership", "PlainProcess"} {
		if strings.Contains(registry, unwanted) {
			t.Fatalf("loom_gen.go has %q", unwanted)
		}
	}
	reg, err := os.ReadFile(filepath.Join(dir, "registry.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"impl.FleetStallProcess(&processes.FleetStall{})", "impl.OrgMembershipPolicy(&policies.OrgMembership{})", "impl.SnapshotProjection(&projections.Snapshot{})", "impl.FleetStall(&aggregates.FleetStall{})"} {
		if !strings.Contains(strings.Join(strings.Fields(string(reg)), " "), strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("registry.go has no %q:\n%s", want, reg)
		}
	}

	// and it compiles
	vetGenerated(t, dir, "example.com/s")
}
