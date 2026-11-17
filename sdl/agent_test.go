package sdl_test

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

func agentSchema(dirs, agent string) string {
	return `
service desk

aggregate Ticket ` + dirs + ` {
  state { status: string }
  command OpenTicket -> TicketOpened
  ` + agent + `
  event TicketOpened { status: string! }
}
`
}

// TestParseAgent: `agent <name> { kind: "<kind>", deadline: <d> }` on a
// @loop aggregate declares an agent step, the comma optional.
func TestParseAgent(t *testing.T) {
	s, err := sdl.Parse(agentSchema("@loop", `agent triage { kind: "triage", deadline: 30m }
  agent sweep { deadline: 2s kind: "sweep" }`))
	if err != nil {
		t.Fatal(err)
	}
	ags := s.Aggregates[0].Agents
	if len(ags) != 2 || ags[0].Name != "triage" || ags[0].Kind != "triage" || ags[0].Deadline != "30m" ||
		ags[1].Name != "sweep" || ags[1].Kind != "sweep" || ags[1].Deadline != "2s" {
		t.Fatalf("agents parsed as %+v %+v", ags[0], ags[1])
	}
	if d, err := ags[0].Duration(); err != nil || d.Minutes() != 30 {
		t.Errorf("triage's deadline is %v (%v), want 30m", d, err)
	}
}

// TestAgentRefusals: an agent belongs to a @loop aggregate and names a
// kind and a deadline; anything else is refused with a reason.
func TestAgentRefusals(t *testing.T) {
	for _, c := range []struct {
		name, dirs, agent, want string
	}{
		{"not a loop", "", `agent triage { kind: "triage", deadline: 30m }`, "belongs to a @loop aggregate"},
		{"no kind", "@loop", `agent triage { deadline: 30m }`, "names no kind"},
		{"no deadline", "@loop", `agent triage { kind: "triage" }`, "has no deadline"},
		{"empty kind", "@loop", `agent triage { kind: "", deadline: 30m }`, "kind is the Mesh job kind"},
		{"bad deadline", "@loop", `agent triage { kind: "triage", deadline: soon }`, "deadline wants a duration"},
		{"unknown key", "@loop", `agent triage { kind: "triage", deadline: 30m, model: "opus" }`, `unknown "model"`},
		{"twice", "@loop", `agent triage { kind: "triage", deadline: 30m }
  agent triage { kind: "triage", deadline: 1h }`, "declared twice"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := sdl.Parse(agentSchema(c.dirs, c.agent))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error containing %q", err, c.want)
			}
		})
	}
}
