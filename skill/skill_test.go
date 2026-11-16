package skill

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/adopt"
)

// sections is every section the skill had before 2026-11-16's additions:
// an edit may add sections, never drop one an agent relies on.
var sections = []string{
	"## The loop",
	"## Never",
	"## Writing handlers",
	"## Reactions",
	"## Calling the outside world: effects",
	"## Doc comments are published",
	"## Commands that cannot be undone take @plan",
	"## A change to who may see what shows in the contract",
	"## A value someone may change is a setting",
	"## Work that recurs is a schedule",
	"## Waiting for something is NotYet",
	"## Reading",
	"## Events change shape with upcasts",
	"## Testing",
	"## When the generated code does not compile",
	"## When a rebase or merge conflicts in loomgen",
}

// TestSkillKeepsItsSections: every earlier section is still there, with
// the @loop section and the adopt table added.
func TestSkillKeepsItsSections(t *testing.T) {
	out := string(Render("v0.0.0-test"))
	for _, h := range append(sections, "## State that waits or must be watched is a @loop", "## Before you hand-write it, check loom has it") {
		if !strings.Contains(out, "\n"+h+"\n") {
			t.Errorf("the skill has no section %q", h)
		}
	}
	if !strings.Contains(out, "v0.0.0-test") {
		t.Error("the skill is not stamped with its loom version")
	}
}

// TestSkillNamesEveryAdoptRule: the skill tells an agent the feature for
// each pattern loom adopt reports.
func TestSkillNamesEveryAdoptRule(t *testing.T) {
	out := string(Render(""))
	for _, r := range adopt.Rules {
		if !strings.Contains(out, "`"+r.ID+"`") {
			t.Errorf("the skill does not name adopt rule %s", r.ID)
		}
	}
}
