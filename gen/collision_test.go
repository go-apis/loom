package gen_test

import (
	"os"
	"os/exec"
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
	var registry string
	for _, w := range res.Written {
		if strings.HasSuffix(w, "registry_gen.go") {
			raw, err := os.ReadFile(w)
			if err != nil {
				t.Fatal(err)
			}
			registry = strings.Join(strings.Fields(string(raw)), " ")
		}
	}
	if registry == "" {
		t.Fatalf("no registry_gen.go in %v", res.Written)
	}
	for _, want := range []string{
		"FleetStall FleetStallHandlers", "FleetStallProcess FleetStallReactions",
		"OrgMembership OrgMembershipHandlers", "OrgMembershipPolicy OrgMembershipReactions",
		"Snapshot SnapshotHandlers", "SnapshotProjection SnapshotFolds",
		"Plain PlainReactions",
		"impl.FleetStallProcess.OnOpened(", "impl.OrgMembershipPolicy.OnJoined(", "impl.SnapshotProjection.OnTaken(", "impl.Plain.OnTaken(",
		"type FleetStallReactions interface", "type OrgMembershipReactions interface", "type SnapshotFolds interface",
	} {
		if !strings.Contains(registry, want) {
			t.Fatalf("registry_gen.go has no %q", want)
		}
	}
	for _, unwanted := range []string{"impl.FleetStall.OnOpened(", "impl.OrgMembership.OnJoined(", "PlainProcess"} {
		if strings.Contains(registry, unwanted) {
			t.Fatalf("registry_gen.go has %q", unwanted)
		}
	}
	reg, err := os.ReadFile(filepath.Join(dir, "registry.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"FleetStallProcess: &processes.FleetStall{}", "OrgMembershipPolicy: &policies.OrgMembership{}", "SnapshotProjection: &projections.Snapshot{}", "FleetStall: &aggregates.FleetStall{}"} {
		if !strings.Contains(strings.Join(strings.Fields(string(reg)), " "), strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("registry.go has no %q:\n%s", want, reg)
		}
	}

	// and it compiles: a module beside the generated code, loom replaced by this tree
	if testing.Short() {
		return
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	gomod := "module example.com/s\n\ngo 1.24\n\nrequire github.com/go-apis/loom v0.0.0\n\nreplace github.com/go-apis/loom => " + root + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go.sum"), src, 0o644); err != nil {
		t.Fatal(err)
	}
	tidy := exec.Command("go", "mod", "tidy")
	// goEnv: a writable module cache even where the inherited one is
	// read-only (see goenv_test.go). Offline as before when the inherited
	// cache is used; a private cache reads through the read-only one.
	tidy.Dir, tidy.Env = dir, goEnv(t, "-mod=mod")
	if goEnvTemp == "" {
		tidy.Env = append(tidy.Env, "GOPROXY=off")
	}
	if out, err := tidy.CombinedOutput(); err != nil {
		t.Fatalf("go mod tidy: %v\n%s", err, out)
	}
	build := exec.Command("go", "vet", "./...")
	build.Dir, build.Env = dir, goEnv(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("the generated package does not compile: %v\n%s", err, out)
	}
}
