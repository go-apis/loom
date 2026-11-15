package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// firingLook is a kiln's look that asks the kiln master before firing:
// it asks once, fires on a yes, and stokes once on anything else.
func firingLook(within time.Duration, atDeadline string) func(ctx context.Context) ([]loom.Command, error) {
	return func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		kiln := loom.CommandBase{Namespace: told.Namespace, AggregateID: told.ID}
		state, _, err := loom.Load(ctx, "Kiln", told.Namespace, told.ID)
		if err != nil {
			return nil, err
		}
		k := state.(*clockgen.Kiln)
		asked, ok := told.Approval("fire")
		switch {
		case !ok:
			return nil, loom.AskApproval(ctx, loom.Approval{
				Name: "fire", What: "Fire the kiln's load", Role: "kiln-master",
				Deadline: told.At.Add(within), AtDeadline: atDeadline,
			})
		case asked.Status == loom.ApprovalApproved && !k.Fired:
			return []loom.Command{&clockgen.RecordKilnFired{CommandBase: kiln}}, nil
		case asked.Settled() && asked.Status != loom.ApprovalApproved && k.Stokes == 0:
			return []loom.Command{&clockgen.StokeKiln{CommandBase: kiln}}, nil
		}
		return nil, nil
	}
}

func kilnState(t *testing.T, ctx context.Context, cli *loom.Client, kiln loom.CommandBase) *clockgen.Kiln {
	t.Helper()
	state, _, err := cli.Load(ctx, "Kiln", kiln.Namespace, kiln.AggregateID)
	if err != nil {
		t.Fatal(err)
	}
	return state.(*clockgen.Kiln)
}

// TestApprovalTakesItsDeadlineAction: a look asks for an approval — what
// is asked, the role that may answer, a deadline, and what happens at
// the deadline. It is kept as a record that can be listed as waiting on
// that role. Nobody answers: at the deadline the record takes its
// declared action, by "deadline", and the owning loop is called and acts
// on how it came out. An answer after the deadline is refused.
func TestApprovalTakesItsDeadlineAction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)
	clock.SetKilnLook(firingLook(1500*time.Millisecond, loom.AtDeadlineReject))

	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	// asked: an open record, waiting on the role, with a wait on the loop
	waitFor(t, ctx, "the approval to be asked", func() bool {
		rec, err := cli.Approval(ctx, "Kiln", kiln.Namespace, kiln.AggregateID, "fire")
		return err == nil && rec != nil
	})
	rec, err := cli.Approval(ctx, "Kiln", kiln.Namespace, kiln.AggregateID, "fire")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != loom.ApprovalOpen || rec.What != "Fire the kiln's load" || rec.Role != "kiln-master" || rec.AtDeadline != loom.AtDeadlineReject || rec.Settled() {
		t.Errorf("the approval as asked: %+v", rec)
	}
	waiting, err := cli.Approvals(ctx, loom.ApprovalQuery{Namespace: "pottery", Role: "kiln-master"})
	if err != nil || len(waiting) != 1 || waiting[0].ID != kiln.AggregateID {
		t.Errorf("what is waiting on the kiln master: %+v %v", waiting, err)
	}
	if others, _ := cli.Approvals(ctx, loom.ApprovalQuery{Namespace: "pottery", Role: "potter"}); len(others) != 0 {
		t.Errorf("what is waiting on a potter: %+v", others)
	}
	if due := dueOf(t, ctx, cli, kiln); len(due.Waits) != 1 || due.Waits[0].Name != "approval:fire" || due.Waits[0].Owner != "role:kiln-master" {
		t.Errorf("the approval's wait: %+v", due.Waits)
	}
	// while it waits, nothing
	time.Sleep(500 * time.Millisecond)
	if k := kilnState(t, ctx, cli, kiln); k.Fired || k.Stokes != 0 {
		t.Fatalf("the kiln moved before the approval was settled: %+v", k)
	}

	// the deadline passes: rejected, by the deadline, and the loop acted on it
	waitFor(t, ctx, "the loop to act on the deadline", func() bool { return kilnState(t, ctx, cli, kiln).Stokes == 1 })
	rec, err = cli.Approval(ctx, "Kiln", kiln.Namespace, kiln.AggregateID, "fire")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != loom.ApprovalRejected || rec.AnsweredBy != loom.ApprovalByDeadline || rec.AnsweredAt == nil || rec.AnsweredAt.Before(rec.Deadline) {
		t.Errorf("the approval after its deadline: %+v", rec)
	}
	if kilnState(t, ctx, cli, kiln).Fired {
		t.Error("a rejected approval fired the kiln")
	}
	if open, _ := cli.Approvals(ctx, loom.ApprovalQuery{Namespace: "pottery"}); len(open) != 0 {
		t.Errorf("still listed as waiting: %+v", open)
	}
	if settled, _ := cli.Approvals(ctx, loom.ApprovalQuery{Namespace: "pottery", Status: loom.ApprovalRejected}); len(settled) != 1 {
		t.Errorf("the rejected approvals: %+v", settled)
	}
	// too late to answer
	master := loom.WithMeta(ctx, loom.Metadata{Actor: "user:master"})
	if _, err := cli.AnswerApproval(master, "Kiln", kiln.Namespace, kiln.AggregateID, "fire", loom.ApprovalAnswer{Approve: true, Roles: []string{"kiln-master"}}); err == nil || !strings.Contains(err.Error(), "rejected already, by deadline") {
		t.Errorf("an answer after the deadline: %v", err)
	}

	// the other two deadline actions
	for action, want := range map[string]string{loom.AtDeadlineApprove: loom.ApprovalApproved, loom.AtDeadlineExpire: loom.ApprovalExpired} {
		clock.SetKilnLook(firingLook(700*time.Millisecond, action))
		k := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
		if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: k}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, ctx, "an approval to "+action+" at its deadline", func() bool {
			rec, err := cli.Approval(ctx, "Kiln", k.Namespace, k.AggregateID, "fire")
			return err == nil && rec != nil && rec.Status == want && rec.AnsweredBy == loom.ApprovalByDeadline
		})
		// approved by the deadline fires it; expired does not
		if action == loom.AtDeadlineApprove {
			waitFor(t, ctx, "the kiln to be fired on a deadline's yes", func() bool { return kilnState(t, ctx, cli, k).Fired })
		} else {
			waitFor(t, ctx, "the loop to act on an expiry", func() bool { return kilnState(t, ctx, cli, k).Stokes == 1 })
			if kilnState(t, ctx, cli, k).Fired {
				t.Error("an expired approval fired the kiln")
			}
		}
	}
}

// TestApprovalAnsweredByItsRole: an approval is answered by someone who
// holds its role, with a reason; the record says who, and the loop is
// called at once, well before the deadline. Someone without the role is
// refused, as is a second answer. An approval that leaves out what is
// asked, who may answer, when, or what happens at the deadline is not
// asked at all.
func TestApprovalAnsweredByItsRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)
	clock.SetKilnLook(firingLook(time.Hour, loom.AtDeadlineExpire))

	yes := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	no := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	for _, kiln := range []loom.CommandBase{yes, no} {
		if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, ctx, "both approvals to be asked", func() bool {
		open, err := cli.Approvals(ctx, loom.ApprovalQuery{Namespace: "pottery"})
		return err == nil && len(open) == 2
	})

	// a potter may not answer what asks for the kiln master
	potter := loom.WithMeta(ctx, loom.Metadata{Actor: "user:potter"})
	if _, err := cli.AnswerApproval(potter, "Kiln", yes.Namespace, yes.AggregateID, "fire", loom.ApprovalAnswer{Approve: true, Roles: []string{"potter"}}); !errors.Is(err, loom.ErrNotYourApproval) {
		t.Errorf("an answer from someone without the role: %v", err)
	}
	if rec, _ := cli.Approval(ctx, "Kiln", yes.Namespace, yes.AggregateID, "fire"); rec.Status != loom.ApprovalOpen {
		t.Fatalf("a refused answer settled the approval: %+v", rec)
	}

	// the kiln master says yes to one and no to the other
	master := loom.WithMeta(ctx, loom.Metadata{Actor: "user:master"})
	began := time.Now()
	rec, err := cli.AnswerApproval(master, "Kiln", yes.Namespace, yes.AggregateID, "fire", loom.ApprovalAnswer{Approve: true, Reason: "the glaze is dry", Roles: []string{"potter", "kiln-master"}})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != loom.ApprovalApproved || rec.AnsweredBy != "user:master" || rec.Reason != "the glaze is dry" || rec.AnsweredAt == nil {
		t.Errorf("the approval as answered: %+v", rec)
	}
	if _, err := cli.AnswerApproval(master, "Kiln", no.Namespace, no.AggregateID, "fire", loom.ApprovalAnswer{Approve: false, Reason: "cracked pots", Roles: []string{"kiln-master"}}); err != nil {
		t.Fatal(err)
	}
	// the loop is called at once on each answer, an hour before the deadline
	waitFor(t, ctx, "the approved kiln to be fired", func() bool { return kilnState(t, ctx, cli, yes).Fired })
	waitFor(t, ctx, "the loop to act on the no", func() bool { return kilnState(t, ctx, cli, no).Stokes == 1 })
	if took := time.Since(began); took > 10*time.Second {
		t.Errorf("the loop took %s to act on an answer", took)
	}
	if kilnState(t, ctx, cli, no).Fired {
		t.Error("a rejected approval fired the kiln")
	}
	if stored, _ := cli.Approval(ctx, "Kiln", no.Namespace, no.AggregateID, "fire"); stored.Status != loom.ApprovalRejected || stored.Reason != "cracked pots" || stored.AnsweredBy != "user:master" {
		t.Errorf("the rejected approval: %+v", stored)
	}
	// answered, its wait is gone; a second answer is refused
	if due, ok, _ := cli.LoopDue(ctx, "Kiln", yes.Namespace, yes.AggregateID); ok && len(due.Waits) != 0 {
		t.Errorf("an answered approval still waits: %+v", due.Waits)
	}
	if _, err := cli.AnswerApproval(master, "Kiln", yes.Namespace, yes.AggregateID, "fire", loom.ApprovalAnswer{Approve: false, Roles: []string{"kiln-master"}}); err == nil || !strings.Contains(err.Error(), "approved already, by user:master") {
		t.Errorf("a second answer: %v", err)
	}
	if _, err := cli.AnswerApproval(master, "Kiln", yes.Namespace, uuid.New(), "fire", loom.ApprovalAnswer{Approve: true, Roles: []string{"kiln-master"}}); err == nil || !strings.Contains(err.Error(), "has no approval") {
		t.Errorf("an answer to an approval that is not there: %v", err)
	}

	// what an approval must say, asked from outside a look
	whole := loom.Approval{Name: "glaze", What: "Glaze the load", Role: "glazer", Deadline: time.Now().Add(time.Hour), AtDeadline: loom.AtDeadlineExpire}
	for name, change := range map[string]func(a *loom.Approval){
		"no name":            func(a *loom.Approval) { a.Name = "" },
		"not what is asked":  func(a *loom.Approval) { a.What = "" },
		"no role":            func(a *loom.Approval) { a.Role = "" },
		"no deadline":        func(a *loom.Approval) { a.Deadline = time.Time{} },
		"no deadline action": func(a *loom.Approval) { a.AtDeadline = "" },
		"an unknown action":  func(a *loom.Approval) { a.AtDeadline = "shrug" },
	} {
		a := whole
		change(&a)
		if err := cli.AskApproval(ctx, "Kiln", yes.Namespace, yes.AggregateID, a); err == nil {
			t.Errorf("an approval with %s was asked", name)
		}
	}
	if err := cli.AskApproval(ctx, "Bell", "tower", uuid.New(), whole); err == nil || !strings.Contains(err.Error(), "no @loop") {
		t.Errorf("an approval on an aggregate with no loop: %v", err)
	}
	if err := loom.AskApproval(ctx, whole); err == nil || !strings.Contains(err.Error(), "outside a look") {
		t.Errorf("AskApproval outside a look: %v", err)
	}
	// asked from outside, it is a record and a wait like any other
	if err := cli.AskApproval(ctx, "Kiln", yes.Namespace, yes.AggregateID, whole); err != nil {
		t.Fatal(err)
	}
	if glaze, _ := cli.Approvals(ctx, loom.ApprovalQuery{Namespace: "pottery", Role: "glazer"}); len(glaze) != 1 || glaze[0].Name != "glaze" {
		t.Errorf("what is waiting on a glazer: %+v", glaze)
	}
}

// TestApprovalsThroughTheGateway: approvals are listed and answered
// through the gateway. The caller must hold the approval's role in the
// namespace; the answer is recorded as theirs.
func TestApprovalsThroughTheGateway(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)
	clock.SetKilnLook(firingLook(time.Hour, loom.AtDeadlineExpire))
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}})
	if err != nil {
		t.Fatal(err)
	}
	ex, _ := loomgql.AsExecutor(gateway)
	as := func(a loomgql.Access) context.Context {
		r, err := ex.Authenticate(httptest.NewRequest(http.MethodPost, "/", nil).WithContext(loomgql.WithAccess(ctx, a)))
		if err != nil {
			t.Fatal(err)
		}
		return r.Context()
	}
	master := as(loomgql.Access{Namespaces: []string{"pottery"}, Mutate: true, Roles: map[string]string{"pottery": "kiln-master"}, Actor: "user:master"})
	potter := as(loomgql.Access{Namespaces: []string{"pottery"}, Mutate: true, Roles: map[string]string{"pottery": "potter"}, Actor: "user:potter"})

	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	list := `{ approvals(namespace: "pottery", role: "kiln-master") { service aggregate id name what role atDeadline status } }`
	waitFor(t, ctx, "the approval to be listed", func() bool {
		res := ex.Exec(master, list, nil)
		rows, _ := res.Data["approvals"].([]any)
		return len(res.Errors) == 0 && len(rows) == 1
	})
	res := ex.Exec(master, list, nil)
	if raw, _ := json.Marshal(res.Data["approvals"]); string(raw) != `[{"aggregate":"Kiln","atDeadline":"expire","id":"`+kiln.AggregateID.String()+`","name":"fire","role":"kiln-master","service":"clock","status":"open","what":"Fire the kiln's load"}]` {
		t.Errorf("the approvals waiting on the kiln master: %s", raw)
	}
	answer := `mutation($id: UUID!) { answerApproval(namespace: "pottery", aggregate: "Kiln", id: $id, name: "fire", approve: true, reason: "ready") { status answeredBy reason } }`
	vars := map[string]any{"id": kiln.AggregateID.String()}
	if res := ex.Exec(potter, answer, vars); len(res.Errors) == 0 || !strings.Contains(res.Errors[0], "not yours to answer") {
		t.Errorf("a potter answering for the kiln master: %v", res.Errors)
	}
	res = ex.Exec(master, answer, vars)
	if raw, _ := json.Marshal(res.Data["answerApproval"]); len(res.Errors) > 0 || string(raw) != `{"answeredBy":"user:master","reason":"ready","status":"approved"}` {
		t.Errorf("the kiln master's answer: %s %v", raw, res.Errors)
	}
	waitFor(t, ctx, "the kiln to be fired on the answer", func() bool { return kilnState(t, ctx, cli, kiln).Fired })
}
