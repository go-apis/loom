package loomtest_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	"github.com/go-apis/loom/loomtest"
	"github.com/go-apis/loom/mesh"
)

func openTicket(t *testing.T) (loom.CommandBase, *loomtest.LoopScenario) {
	t.Helper()
	ticket := loom.CommandBase{Namespace: "desk", AggregateID: uuid.New()}
	reader := (&loomtest.Reader{}).AddAggregate("Ticket", "desk", ticket.AggregateID, &clockgen.Ticket{Status: "open"})
	return ticket, loomtest.Loop(t, clock.NewRegistry(), clock.DecideTicket).Reading(reader).Instance("Ticket", "desk", ticket.AggregateID)
}

// TestALookAsksAnAgentAndDecidesOnItsAnswer: with no database and no
// node, a fake Mesh takes the triage job an open ticket's look asks for
// — one job, however often the look asks while it is open — and the
// look closes the ticket with the verdict once the fake reports it.
func TestALookAsksAnAgentAndDecidesOnItsAnswer(t *testing.T) {
	ticket, loop := openTicket(t)
	fake := &mesh.Fake{}
	loop.Mesh(fake, "acme")

	loop.Look(noon).ThenNothing()
	if asked := loop.Asked(); len(asked) != 1 || asked[0].Def.Kind != "triage" {
		t.Fatalf("the look asked %+v, want the triage agent", asked)
	}
	offers := fake.Offers()
	if len(offers) != 1 || offers[0].Kind != "triage" || offers[0].Tenant != "acme" {
		t.Fatalf("offers %+v, want one triage job for acme", offers)
	}
	if next := loop.NextLook(); !next.Equal(noon.Add(time.Minute)) {
		t.Errorf("next look at %s, want a minute on: the 30m wait is later", next)
	}

	// still waiting: asked again, nothing new offered
	loop.Look(noon.Add(time.Minute)).ThenNothing()
	if n := len(fake.Offers()); n != 1 {
		t.Fatalf("%d jobs offered, want 1", n)
	}

	job := loop.Agents()["triage"].JobID
	if err := fake.Finish(context.Background(), job, json.RawMessage(`{"verdict":"duplicate"}`)); err != nil {
		t.Fatal(err)
	}
	loop.Look(noon.Add(61 * time.Second)).Then(&clockgen.CloseTicket{CommandBase: ticket, Verdict: "duplicate"})
	if a, _ := loop.Told().Agent("triage"); a.Status != loom.AgentFinished {
		t.Errorf("the look was told the agent is %s, want finished", a.Status)
	}
}

// TestALookDecidesFromARecordedAgentOutcome: an outcome recorded for the
// scenario, with no Mesh at all, is what the look reads: a failed job
// closes the ticket "failed"; an agent that never answers, once its 30m
// pass, closes it "unanswered".
func TestALookDecidesFromARecordedAgentOutcome(t *testing.T) {
	ticket, loop := openTicket(t)
	loop.AgentAnswered(loom.AgentRecord{Name: "triage", Kind: "triage", Status: loom.AgentFailed,
		Outcome: &mesh.Outcome{End: mesh.EndFailed, Cause: "work", Reason: "the agent crashed"}})
	loop.Look(noon).Then(&clockgen.CloseTicket{CommandBase: ticket, Verdict: "failed"})

	ticket, loop = openTicket(t)
	loop.Look(noon).ThenNothing() // asks; no Mesh, so recorded only
	if rec := loop.Agents()["triage"]; rec.Status != loom.AgentRequested || !rec.Deadline.Equal(noon.Add(30*time.Minute)) {
		t.Fatalf("the request is %+v, want open until 30m on", rec)
	}
	if next := loop.LookedAt(noon.Add(29 * time.Minute)).NextLook(); !next.Equal(noon.Add(30 * time.Minute)) {
		t.Errorf("next look at %s, want the agent's deadline", next)
	}
	loop.Look(noon.Add(30 * time.Minute)).Then(&clockgen.CloseTicket{CommandBase: ticket, Verdict: "unanswered"})
	if _, late := loop.Told().ExpiredWait(loom.AgentWait("triage")); !late {
		t.Error("the look was not told the agent's wait expired")
	}
}
