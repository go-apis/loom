// Package skill is the Claude Code skill loom writes into a consumer
// repository (ADR 0007): what an agent working in a loom service must
// know — the loop, the pinned generator, the naming and effect rules —
// stamped with the loom version that wrote it, so the advice matches the
// generator the repository pins and `loom skill --check` can say when it
// is stale.
package skill

import (
	_ "embed"
	"strings"
)

//go:embed files/SKILL.md
var template string

// Path is where the skill lives, relative to the repository root: the
// project skills directory Claude Code reads.
const Path = ".claude/skills/loom/SKILL.md"

// Render is the skill for loom version (as `go version -m` names it,
// e.g. v0.68.0; "(devel)" for a local build).
func Render(version string) []byte {
	if version == "" {
		version = "(devel)"
	}
	return []byte(strings.ReplaceAll(template, "{{LOOM_VERSION}}", version))
}
