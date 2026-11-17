package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	loomgraphql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	"github.com/go-apis/loom/mesh"
)

var meshSecret = []byte("hook-secret")

// agentClock is a clock client that asks its agents of fake, with the
// hook mounted at the gateway the way a service mounts it; ctx ending
// stops it.
func agentClock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fake *mesh.Fake) *loom.Client {
	t.Helper()
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry(), Mesh: fake, MeshTenant: "acme", MeshHookSecret: meshSecret})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return cli
}

func agentWait(t *testing.T, ctx context.Context, cli *loom.Client, ticket loom.CommandBase, name string) (loom.Wait, bool) {
	t.Helper()
	due, ok, err := cli.LoopDue(ctx, "Ticket", ticket.Namespace, ticket.AggregateID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return loom.Wait{}, false
	}
	for _, w := range due.Waits {
		if w.Name == loom.AgentWait(name) {
			return w, true
		}
	}
	return loom.Wait{}, false
}

func agentRecord(t *testing.T, ctx context.Context, cli *loom.Client, ticket loom.CommandBase, name string) *loom.AgentRecord {
	t.Helper()
	rec, err := cli.Agent(ctx, "Ticket", ticket.Namespace, ticket.AggregateID, name)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// TestALoopAsksAnAgentThroughMesh: a ticket's look asks its triage
// agent. The ask is recorded, exactly one Mesh job of kind triage is
// offered, under an idempotency key, and the ticket waits on mesh for
// the declared 30m. Asking again — after a restart, and after a crash
// between recording and offering — offers no second job. Mesh's signed
// hook reports the job finished, through the gateway: the outcome is
// recorded, the wait ends, and the look sees the verdict within about a
// second and closes the ticket with it. A hook not signed by Mesh is
// refused, and one heard twice settles once.
func TestALoopAsksAnAgentThroughMesh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	clock.SetTicketScript(nil)
	pool := testDB(t, ctx)
	fake := &mesh.Fake{Secret: meshSecret}
	firstCtx, stopFirst := context.WithCancel(ctx)
	first := agentClock(t, firstCtx, pool, fake)
	fake.Hook = loomgraphql.MeshHooks(first)

	ticket := loom.CommandBase{Namespace: "desk", AggregateID: uuid.New()}
	if err := first.Dispatch(ctx, &clockgen.OpenTicket{CommandBase: ticket}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the triage job to be offered", func() bool {
		rec := agentRecord(t, ctx, first, ticket, "triage")
		return rec != nil && rec.JobID != ""
	})
	rec := agentRecord(t, ctx, first, ticket, "triage")
	if rec.Status != loom.AgentRequested || rec.Kind != "triage" || rec.Attempt != 1 || rec.Deadline.Sub(rec.AskedAt) != 30*time.Minute {
		t.Errorf("the request is %+v, want requested, kind triage, attempt 1, 30m to answer", rec)
	}
	offers := fake.Offers()
	if len(offers) != 1 {
		t.Fatalf("%d jobs offered, want 1", len(offers))
	}
	o := offers[0]
	if o.Kind != "triage" || o.Tenant != "acme" || o.Subject != (mesh.Subject{Type: "Ticket", ID: ticket.AggregateID.String()}) ||
		o.IdempotencyKey != rec.IdempotencyKey("clock") || !strings.Contains(string(o.Refs), `"agent":"triage"`) {
		t.Errorf("the offer is %+v", o)
	}
	w, ok := agentWait(t, ctx, first, ticket, "triage")
	if !ok || w.Owner != loom.AgentOwner || !w.Deadline.Equal(rec.Deadline) {
		t.Fatalf("the ticket's wait is %+v (%v), want one owned by mesh until %s", w, ok, rec.Deadline)
	}

	// a restart: a new client on the same store looks again, for an
	// event, and asks again; the request is open, so no job is offered
	stopFirst()
	time.Sleep(200 * time.Millisecond)
	cli := agentClock(t, ctx, pool, fake)
	fake.Hook = loomgraphql.MeshHooks(cli)
	looks := len(clock.TicketToldOf(ticket.AggregateID.String()))
	if err := cli.Dispatch(ctx, &clockgen.OpenTicket{CommandBase: ticket}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "a look after the restart", func() bool { return len(clock.TicketToldOf(ticket.AggregateID.String())) > looks })
	// a crash between recording the ask and offering its job: the next
	// look offers it again, under the same key, and Mesh answers the
	// same job
	if _, err := pool.Exec(ctx, `UPDATE loom_agents SET job_id='' WHERE aggregate_id=$1`, ticket.AggregateID); err != nil {
		t.Fatal(err)
	}
	if err := cli.Dispatch(ctx, &clockgen.OpenTicket{CommandBase: ticket}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the job to be offered again", func() bool { return agentRecord(t, ctx, cli, ticket, "triage").JobID != "" })
	if n := len(fake.Offers()); n != 1 {
		t.Fatalf("%d jobs offered after a restart and a re-offer, want 1", n)
	}
	if again := agentRecord(t, ctx, cli, ticket, "triage"); again.JobID != rec.JobID || again.Attempt != 1 {
		t.Fatalf("the request is now %+v, want job %s, attempt 1", again, rec.JobID)
	}

	// a hook Mesh did not sign is refused, and settles nothing
	forged, _ := json.Marshal(mesh.JobChange{Seq: 99, Event: mesh.EventJobEnded, Outcome: &mesh.Outcome{End: mesh.EndFinished},
		Job: mesh.JobRef{ID: rec.JobID, Kind: "triage", Refs: o.Refs}})
	if err := mesh.Deliver(ctx, cli.MeshHook(), []byte("not-the-secret"), mesh.EventJobEnded, "1", forged); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a forged hook answered %v, want 401", err)
	}
	if agentRecord(t, ctx, cli, ticket, "triage").Settled() {
		t.Fatal("a forged hook settled the request")
	}

	// Mesh reports the job finished: recorded, the wait ends, and the
	// look closes the ticket with the verdict within about a second
	finished := time.Now()
	if err := fake.Finish(ctx, rec.JobID, json.RawMessage(`{"verdict":"bug"}`)); err != nil {
		t.Fatal(err)
	}
	got := agentRecord(t, ctx, cli, ticket, "triage")
	var verdict clock.TriageResult
	if got.Status != loom.AgentFinished || got.Result(&verdict) != nil || verdict.Verdict != "bug" || got.EndedAt == nil {
		t.Fatalf("the request is %+v after the hook, want finished with the result", got)
	}
	if _, ok := agentWait(t, ctx, cli, ticket, "triage"); ok {
		t.Error("the wait is still held after Mesh answered")
	}
	var closedAt time.Time
	waitFor(t, ctx, "the ticket closed with the verdict", func() bool {
		st, _, err := cli.Load(ctx, "Ticket", ticket.Namespace, ticket.AggregateID)
		if err != nil {
			t.Fatal(err)
		}
		if tk := st.(*clockgen.Ticket); tk.Status == "closed" && tk.Verdict == "bug" {
			closedAt = time.Now()
			return true
		}
		return false
	})
	if took := closedAt.Sub(finished); took > 1500*time.Millisecond {
		t.Errorf("the look saw the outcome %s after Mesh answered, want within about a second", took)
	}
	var saw bool
	for _, told := range clock.TicketToldOf(ticket.AggregateID.String()) {
		if a, ok := told.Agent("triage"); ok && a.Status == loom.AgentFinished {
			saw = true
		}
	}
	if !saw {
		t.Error("no look was told the triage agent finished")
	}

	// the same hook again (Mesh retries until answered): settled once
	body, _ := json.Marshal(mesh.JobChange{Seq: 1, Event: mesh.EventJobEnded, Outcome: &mesh.Outcome{End: mesh.EndFailed},
		Job: mesh.JobRef{ID: rec.JobID, Kind: "triage", Refs: o.Refs}})
	if err := mesh.Deliver(ctx, fake.Hook, meshSecret, mesh.EventJobEnded, "1", body); err != nil {
		t.Fatal(err)
	}
	if again := agentRecord(t, ctx, cli, ticket, "triage"); again.Status != loom.AgentFinished {
		t.Errorf("a hook heard twice changed the request to %s", again.Status)
	}
}

// TestAnAgentThatNeverAnswersExpires: a ticket asks its sweep agent,
// declared with 2s to answer, and Mesh never reports: the wait expires,
// the request is expired and its job cancelled, and the look is told —
// once.
func TestAnAgentThatNeverAnswersExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var (
		mu      sync.Mutex
		expired []loom.AgentRecord
	)
	clock.SetTicketScript(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		if _, late := told.ExpiredWait(loom.AgentWait("sweep")); late {
			rec, _ := told.Agent("sweep")
			mu.Lock()
			expired = append(expired, rec)
			mu.Unlock()
			return nil, nil
		}
		if _, asked := told.Agent("sweep"); asked {
			return nil, nil
		}
		return nil, clockgen.AskTicketSweep(ctx, loom.AgentAsk{Tenant: "globex"})
	})
	t.Cleanup(func() { clock.SetTicketScript(nil) })
	pool := testDB(t, ctx)
	fake := &mesh.Fake{Secret: meshSecret}
	cli := agentClock(t, ctx, pool, fake)
	fake.Hook = cli.MeshHook()

	ticket := loom.CommandBase{Namespace: "desk", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.OpenTicket{CommandBase: ticket}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the sweep job to be offered", func() bool {
		rec := agentRecord(t, ctx, cli, ticket, "sweep")
		return rec != nil && rec.JobID != ""
	})
	if o := fake.Offers(); len(o) != 1 || o[0].Kind != "sweep" || o[0].Tenant != "globex" {
		t.Fatalf("offers %+v, want one sweep job for globex", o)
	}
	w, ok := agentWait(t, ctx, cli, ticket, "sweep")
	if !ok || w.Owner != loom.AgentOwner {
		t.Fatalf("the ticket's wait is %+v (%v), want one owned by mesh", w, ok)
	}
	waitFor(t, ctx, "the look to be told the agent did not answer", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(expired) > 0
	})
	mu.Lock()
	told := expired[0]
	mu.Unlock()
	if told.Status != loom.AgentExpired {
		t.Errorf("the look was told the sweep is %s, want expired", told.Status)
	}
	rec := agentRecord(t, ctx, cli, ticket, "sweep")
	if rec.Status != loom.AgentExpired || rec.EndedAt == nil {
		t.Errorf("the request is %+v, want expired", rec)
	}
	if _, ok := fake.Cancelled(rec.JobID); !ok {
		t.Error("the expired agent's job was not cancelled")
	}
	if _, ok := agentWait(t, ctx, cli, ticket, "sweep"); ok {
		t.Error("the expired wait is still held")
	}
	// a late answer is not read
	if err := fake.Finish(ctx, rec.JobID, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if late := agentRecord(t, ctx, cli, ticket, "sweep"); late.Status != loom.AgentExpired {
		t.Errorf("a late answer changed the request to %s", late.Status)
	}
	// told once: a look for an event later is not told again
	if err := cli.Dispatch(ctx, &clockgen.OpenTicket{CommandBase: ticket}); err != nil {
		t.Fatal(err)
	}
	looks := len(clock.TicketToldOf(ticket.AggregateID.String()))
	waitFor(t, ctx, "a look for the event", func() bool { return len(clock.TicketToldOf(ticket.AggregateID.String())) > looks })
	mu.Lock()
	n := len(expired)
	mu.Unlock()
	if n != 1 {
		t.Errorf("the look was told %d times the agent expired, want once", n)
	}
}

// TestTheMeshHookRefusesWhatIsNotAFeedEntry: the hook verifies before it
// reads, refuses a body that is not a feed entry, and answers a confirm
// ask for a request no longer open with cancel.
func TestTheMeshHookRefusesWhatIsNotAFeedEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli := agentClock(t, ctx, pool, &mesh.Fake{Secret: meshSecret})
	h := cli.MeshHook()
	post := func(body []byte, sig string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/hooks/mesh", strings.NewReader(string(body)))
		req.Header.Set(mesh.HeaderSignature, sig)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	if rec := post([]byte(`{"seq":1,"event":"job.ended"}`), "sha256=00"); rec.Code != http.StatusUnauthorized {
		t.Errorf("an unsigned hook answered %d, want 401", rec.Code)
	}
	bad := []byte(`{"event":"job.ended"}`)
	if rec := post(bad, mesh.Sign(meshSecret, bad)); rec.Code != http.StatusBadRequest {
		t.Errorf("a body with no seq answered %d, want 400", rec.Code)
	}
	refs, _ := json.Marshal(map[string]any{"loom": map[string]any{"service": "clock", "aggregate": "Ticket", "namespace": "desk", "id": uuid.New(), "agent": "triage", "attempt": 1}})
	ask, _ := json.Marshal(mesh.ConfirmAsk{Event: mesh.EventJobConfirm, Job: mesh.JobRef{ID: "job-x", Kind: "triage", Refs: refs}})
	rec := post(ask, mesh.Sign(meshSecret, ask))
	var reply mesh.ConfirmReply
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &reply) != nil || reply.Answer != "cancel" {
		t.Errorf("a confirm ask for no open request answered %d %s, want cancel", rec.Code, rec.Body.String())
	}
}
