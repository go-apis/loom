package loom

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestDispatchIsRefusedWhilePlanning: code a plan is running returns
// its commands. Dispatching them itself would do what the plan is only
// working out, so Dispatch under a plan's context is refused before it
// touches anything.
func TestDispatchIsRefusedWhilePlanning(t *testing.T) {
	ctx := context.WithValue(context.Background(), planKey{}, &planRun{})
	if !Planning(ctx) {
		t.Fatal("a plan's context does not say it is planning")
	}
	err := (&Client{}).Dispatch(ctx)
	if err == nil || !strings.Contains(err.Error(), "while planning") {
		t.Errorf("Dispatch under a plan = %v, want a refusal", err)
	}
}

type plannedCmd struct {
	CommandBase
	Note string `json:"note"`
}

func (*plannedCmd) LoomCommand() string { return "Note" }

// TestPlannedDispatchesSayWhatBecomesOfEach: a command a reaction
// answers with is run, held by a timer, a timer's cancellation, or
// queued in a batch, and a plan shows which, under the command's own
// name and target.
func TestPlannedDispatchesSayWhatBecomesOfEach(t *testing.T) {
	id := uuid.New()
	cmd := func(note string) Command {
		return &plannedCmd{CommandBase: CommandBase{Namespace: "acme", AggregateID: id}, Note: note}
	}
	at := time.Date(2026, 11, 15, 3, 0, 0, 0, time.UTC)
	got := plannedDispatches([]Command{cmd("now"), At(cmd("later"), at), CancelTimer(cmd("never")), AsBatch(cmd("one"), cmd("two"))})
	var said []string
	for _, d := range got {
		s := d.How + " " + d.Command + " " + d.Data.(*plannedCmd).Note
		if d.Namespace != "acme" || d.TargetID != id {
			t.Errorf("%s addresses %s/%s", s, d.Namespace, d.TargetID)
		}
		if d.At != nil {
			s += " at " + d.At.Format(time.RFC3339)
		}
		said = append(said, s)
	}
	want := "run Note now; timer Note later at 2026-11-15T03:00:00Z; cancel_timer Note never; batch Note one; batch Note two"
	if got := strings.Join(said, "; "); got != want {
		t.Errorf("planned dispatches:\n got %s\nwant %s", got, want)
	}
}
