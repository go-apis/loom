package skill

import (
	"regexp"
	"strings"
	"testing"
)

var headingRe = regexp.MustCompile(`(?m)^#{1,2} .+$`)

// sections the skill had before the @loop and waits section was added and
// the adopt rules were named: Render's output must keep every one of
// these headings.
var sectionsBefore = []string{
	"# loom",
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

// sectionsAdded are the new headings: a service's @loop aggregates and the
// waits they keep.
var sectionsAdded = []string{
	"## `@loop` aggregates and waits",
}

func TestSkillKeepsItsSections(t *testing.T) {
	out := string(Render("v1.2.3"))
	have := map[string]bool{}
	for _, h := range headingRe.FindAllString(out, -1) {
		have[strings.TrimSpace(h)] = true
	}
	for _, h := range sectionsBefore {
		if !have[h] {
			t.Errorf("section the skill had before is missing: %q", h)
		}
	}
	for _, h := range sectionsAdded {
		if !have[h] {
			t.Errorf("new section is missing: %q", h)
		}
	}
}

func TestSkillNamesTheFeatureForEachAdoptRule(t *testing.T) {
	out := string(Render("v1.2.3"))
	for _, rule := range []struct{ id, feature string }{
		{"ticker-goroutine", "Client.Every"},
		{"self-rearming-timer", "schedule"},
		{"timer-policy", "@loop"},
	} {
		if !strings.Contains(out, rule.id) {
			t.Errorf("skill never names adopt rule %q", rule.id)
		}
		if !strings.Contains(out, rule.feature) {
			t.Errorf("skill never names feature %q for adopt rule %q", rule.feature, rule.id)
		}
	}
}
