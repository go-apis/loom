package loomtest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/mesh"
)

// --- loops ---

// LoopScenario tests a @loop aggregate's look with no database: when it
// would be looked at next, what it is told when it is, and what it
// decides. Each look is a loom.LookTurn, the same the runner makes, so
// the declared deadlines, the waits a look starts and ends, LookAgain,
// LookDone, AskApproval, the backoff and the parking all follow the
// runtime's own rule. The commands a look answers with are run through
// the registry's handlers and their events folded into the reader's
// state, so a second look sees what the first one did, as it would in
// the store; a command scheduled with loom.At or loom.After is held
// until its time (Timers).
//
// A scenario is driven one of two ways. Look(at) makes one look at a
// time the test names, and reads no clock. RunFor and RunUntil run the
// clock: they sleep to each look the rule makes due and to each timer,
// and make it, so a test that runs them inside a synctest bubble
// (synctest.Test) walks hours or weeks of looks in no real time and in
// the same order every run:
//
//	synctest.Test(t, func(t *testing.T) {
//		loop := loomtest.LoopOf(t, reg, "Kiln", "pottery", id).Reading(reader)
//		loop.RunFor(7 * time.Second) // looks at 0s, 1s, 3s, 7s while it fails
//	})
//
// Outside a bubble RunFor sleeps for real.
//
// What the runner does with the store is not here: claiming and leasing
// instances, the newest-generation rule, the look's commands as one
// unit of work (here each is handled in turn, and the first refused
// fails the look), the reactions their events start, and supervisors'
// children (Looking.Children stays empty).
type LoopScenario struct {
	t      *testing.T
	reg    *loom.Registry
	look   func(ctx context.Context) ([]loom.Command, error)
	reader *Reader
	timers *TimerScenario

	// the instance looked at, when the scenario names one
	def *loom.AggregateDef
	ns  string
	id  uuid.UUID

	limit     int
	state     loom.LoopState
	asked     time.Time // a look due sooner than the rule says: LookAgain, or an answer
	fired     []string
	approvals map[string]loom.ApprovalRecord
	// the instance's agent requests, the agents its last look asked, and
	// the fake Mesh their jobs are offered to (Mesh)
	agents  map[string]loom.AgentRecord
	asks    []loom.AgentAsked
	mesh    *mesh.Fake
	tenant  string
	done    bool
	made    []LookMade
	looking bool

	now    time.Time
	told   loom.Looking
	cmds   []loom.Command
	events []any
	err    error
	ran    bool
}

// LookMade is one look a scenario made, as it came out.
type LookMade struct {
	// At is when the look was made.
	At time.Time
	// Told is what the look was told.
	Told loom.Looking
	// Commands is what it answered with, and Events what they emitted.
	Commands []loom.Command
	Events   []any
	// Err is why it failed; nil when it did not.
	Err error
	// State is the instance's loop state after the look, and Next when
	// it is next looked at.
	State loom.LoopState
	Next  time.Time
	// Done is whether the look said LookDone and no event has arrived
	// since: the instance is not looked at again until one does.
	Done bool
}

// Loop starts a scenario for one look: the method the generated Look
// interface gives a @loop aggregate (look.LookOrder).
func Loop(t *testing.T, reg *loom.Registry, look func(ctx context.Context) ([]loom.Command, error)) *LoopScenario {
	t.Helper()
	s := &LoopScenario{t: t, reg: reg, look: look, reader: &Reader{}, limit: loom.LoopFailureLimit, approvals: map[string]loom.ApprovalRecord{}}
	s.timers = Timers(t, reg).Reading(s.reader)
	s.timers.noted = s.noteEvents
	return s
}

// LoopOf starts a scenario for one instance of a @loop aggregate, looked
// at by the look the registry wires for it (Impl.Look).
func LoopOf(t *testing.T, reg *loom.Registry, aggregate, namespace string, id uuid.UUID) *LoopScenario {
	t.Helper()
	for _, a := range reg.Aggregates {
		if a.Name == aggregate && a.Look != nil {
			return Loop(t, reg, a.Look).Instance(aggregate, namespace, id)
		}
	}
	t.Fatalf("loomtest: no @loop aggregate %q", aggregate)
	return nil
}

// Instance names the instance looked at: the look is told its name, its
// declared deadlines (`deadline ... while ...`) are kept over its state
// in the reader, and only the events its commands emit on it, or that
// are dispatched to it, count as events for the loop. Without it, an
// event on any @loop aggregate counts.
func (s *LoopScenario) Instance(aggregate, namespace string, id uuid.UUID) *LoopScenario {
	s.t.Helper()
	for _, a := range s.reg.Aggregates {
		if a.Name == aggregate {
			s.def, s.ns, s.id = a, namespace, id
			return s
		}
	}
	s.t.Fatalf("loomtest: no aggregate %q", aggregate)
	return s
}

// Reading injects the state the look reads with loom.Load. The commands
// it answers with change that state.
func (s *LoopScenario) Reading(r *Reader) *LoopScenario {
	if r != nil {
		s.reader = r
		s.timers.reader = r
	}
	return s
}

// FailureLimit is how many looks in a row may fail before the instance
// is parked (loom.Config.LoopFailureLimit); loom.LoopFailureLimit
// unless set.
func (s *LoopScenario) FailureLimit(n int) *LoopScenario {
	s.limit = n
	return s
}

// LookedAt says when the loop was last looked at.
func (s *LoopScenario) LookedAt(at time.Time) *LoopScenario {
	s.state.Looked = at
	return s
}

// EventAt says an event the loop cares about arrived at at.
func (s *LoopScenario) EventAt(at time.Time) *LoopScenario {
	s.restart()
	s.state.Event = at
	return s
}

// restart starts the looks of an instance a look said was done afresh,
// as the runtime does when an event arrives for it: nothing is kept of
// its looks before, and it is due at once.
func (s *LoopScenario) restart() {
	if s.done {
		s.state, s.fired, s.asked, s.done = loom.LoopState{}, nil, time.Time{}, false
	}
}

// Failing says the loop's last n looks failed in a row.
func (s *LoopScenario) Failing(n int) *LoopScenario {
	s.state.Failures = n
	return s
}

// Failures is how many looks in a row have failed, the scenario's own
// looks included: a look that errs adds one, and one that does not
// clears them.
func (s *LoopScenario) Failures() int {
	return s.state.Failures
}

// Parked reports whether the instance is parked: its looks failed the
// failure limit in a row (loom.LoopState.ParkPast).
func (s *LoopScenario) Parked() bool {
	return s.state.Parked
}

// State is what the runtime would hold about the instance now.
func (s *LoopScenario) State() loom.LoopState {
	return s.state
}

// Done reports whether a look said LookDone and no event has arrived
// since: the runtime looks at it no more.
func (s *LoopScenario) Done() bool {
	return s.done
}

// Waiting starts a wait from outside a look, as Client.Wait does: it is
// added, or replaces its namesake. An instance a look said was done is
// given its looks again, the next a minute on (the clock's now).
func (s *LoopScenario) Waiting(w loom.Wait) *LoopScenario {
	if s.done {
		s.restart()
		s.state.Looked = time.Now()
	}
	s.state = s.state.Waited(w)
	return s
}

// EndWait ends the wait of that name from outside a look, as
// Client.EndWait does: it has been answered. Ending a parked instance's
// loom.LoopParkedWait unparks it, its failures cleared, and makes it due
// at once (the clock's now).
func (s *LoopScenario) EndWait(name string) *LoopScenario {
	if s.done {
		return s
	}
	var now bool
	if s.state, now = s.state.WaitEnded(name); now {
		s.asked = time.Now()
	}
	return s
}

// Approval is the instance's approval of that name as it stands, if a
// look has asked for it.
func (s *LoopScenario) Approval(name string) (loom.ApprovalRecord, bool) {
	r, ok := s.approvals[name]
	return r, ok
}

// Answer answers an open approval at the clock's now, as
// Client.AnswerApproval does: actor must hold its role, and it must be
// open and before its deadline. An answer is news for the loop: its
// wait ends and the instance is looked at at once.
func (s *LoopScenario) Answer(name, actor string, answer loom.ApprovalAnswer) (loom.ApprovalRecord, error) {
	rec, ok := s.approvals[name]
	if !ok {
		return rec, fmt.Errorf("loomtest: no approval %q", name)
	}
	now := time.Now()
	rec, err := rec.Answer(actor, answer, now)
	if err != nil {
		return rec, err
	}
	s.approvals[name] = rec
	if !s.done {
		s.eventAt(now)
		s.asked = now
		s.state, _ = s.state.WaitEnded(loom.ApprovalWait(name))
	}
	return rec, nil
}

// Dispatch runs commands from outside a look at the clock's now, as a
// caller of Client.Dispatch would: through the registry's handlers into
// the reader's state, a scheduled one held until its time. An event they
// emit on the instance brings its next look to within loom.LoopSoon, and
// starts the looks again after LookDone. An event at the very instant of
// the last look is noted a microsecond after it, as the store would
// note it: its look is LoopSoon and a microsecond on.
func (s *LoopScenario) Dispatch(cmds ...loom.Command) *LoopScenario {
	s.t.Helper()
	now := time.Now()
	for _, cmd := range cmds {
		if _, err := s.dispatch(cmd, now); err != nil {
			s.t.Fatalf("loomtest: %s: %v", cmd.LoomCommand(), err)
		}
	}
	return s
}

// Timers is the scenario's timers: what its looks and Dispatch
// scheduled, and what has fired.
func (s *LoopScenario) Timers() *TimerScenario {
	return s.timers
}

// NextLook is when the runtime would look next, by the same rule it
// uses (loom.LoopState.NextLook, or sooner when a look asked with
// LookAgain or an answer made it due). The zero time is at once.
func (s *LoopScenario) NextLook() time.Time {
	next := s.state.NextLook()
	if !s.asked.IsZero() && s.asked.Before(next) {
		next = s.asked
	}
	return next
}

// Look calls the look as the runtime would at now (loom.StartLook): the
// declared deadlines kept, told whether an event arrived, which waits
// have expired, and its approvals as they stand, those past their
// deadline having taken their deadline action. What it dispatches is
// handled and folded into the reader's state; the look is then ended as
// the runner ends it (loom.LookTurn.End: failed when it erred, panicked
// or a command it answered with was refused, and parked past the
// failure limit).
func (s *LoopScenario) Look(now time.Time) *LoopScenario {
	s.t.Helper()
	s.now, s.ran = now, true
	s.settleFromMesh(now)
	var (
		defs []loom.DeadlineDef
		agg  loom.AggregateState
	)
	if s.def != nil && len(s.def.Deadlines) > 0 {
		defs, agg = s.def.Deadlines, s.reader.Aggregates[key(s.def.Name, s.ns, s.id)]
		if agg == nil {
			agg = s.def.NewState()
		}
	}
	turn := loom.StartLook(s.state, s.fired, defs, agg, now)
	if s.def != nil {
		turn.Told.Aggregate, turn.Told.Namespace, turn.Told.ID = s.def.Name, s.ns, s.id
	}
	// an approval nobody answered takes its deadline action before the
	// look that is told its wait has expired
	for _, w := range turn.Told.Expired {
		for name, rec := range s.approvals {
			if loom.ApprovalWait(name) == w.Name {
				s.approvals[name], _ = rec.PastDeadline(now)
			}
		}
	}
	turn.Told.Approvals = s.approvalList()
	// an agent that did not answer in time is expired before the look
	// that is told its wait has, and the look is told how its agents stand
	s.expireAgents(turn.Told.Expired, now)
	turn.Told.Agents = s.agentList()
	var agentDefs []loom.AgentDef
	if s.def != nil {
		agentDefs = s.def.Agents
	}
	turn.Mesh(agentDefs, true, cmpOr(s.tenant, "loomtest"))
	s.told = turn.Told

	s.cmds, s.err = s.call(turn.Context(loom.WithStateReader(context.Background(), s.reader)))
	s.events = nil
	s.looking = true
	if s.err == nil {
		for _, cmd := range s.cmds {
			events, err := s.dispatch(cmd, now)
			if err != nil {
				s.err = fmt.Errorf("%s: %w", cmd.LoomCommand(), err)
				break
			}
			s.events = append(s.events, events...)
		}
	}
	s.looking = false
	var next time.Time
	s.state, next = turn.End(s.state, s.limit, s.err)
	s.asked = time.Time{}
	if next.Before(s.state.NextLook()) {
		s.asked = next
	}
	s.fired = turn.Fired
	s.asks = nil
	if s.err == nil {
		s.asks = turn.Agents()
		if err := s.openAgents(now); err != nil {
			s.t.Fatalf("loomtest: offering an agent's job: %v", err)
		}
	}
	for _, a := range turn.Approvals() {
		s.approvals[a.Name] = loom.ApprovalRecord{Approval: a, Aggregate: s.told.Aggregate, Namespace: s.told.Namespace, ID: s.told.ID, Status: loom.ApprovalOpen, AskedAt: now}
	}
	// done, unless an event has arrived since the look began
	s.done = turn.Done() && !s.state.Event.After(now)
	s.made = append(s.made, LookMade{At: now, Told: s.told, Commands: s.cmds, Events: s.events, Err: s.err, State: s.state, Next: next, Done: s.done})
	return s
}

// call makes the look; one that panics has failed, as in the runner.
func (s *LoopScenario) call(ctx context.Context) (cmds []loom.Command, err error) {
	defer func() {
		if r := recover(); r != nil {
			cmds, err = nil, fmt.Errorf("loomtest: the look panicked: %v", r)
		}
	}()
	return s.look(ctx)
}

// dispatch runs one command at now: a scheduled one or a cancel goes to
// the timers, anything else through its handler.
func (s *LoopScenario) dispatch(cmd loom.Command, now time.Time) ([]any, error) {
	if s.timers.take(cmd) {
		return nil, nil
	}
	return s.timers.apply(cmd, now)
}

// noteEvents notes events emitted on an aggregate at at: an event for
// the loop when they are on the instance (or on any @loop aggregate,
// when the scenario names none).
func (s *LoopScenario) noteEvents(agg *loom.AggregateDef, ns string, id uuid.UUID, at time.Time) {
	if s.def != nil && (agg.Name != s.def.Name || ns != s.ns || id != s.id) {
		return
	}
	if s.def == nil && agg.Look == nil {
		return
	}
	s.eventAt(at)
}

// eventAt notes an event that arrived at at. One the look's own commands
// emit lands after the instant the look is of, and one from outside
// after the look it follows: the store's clock has moved on, by its
// smallest step, where the bubble's has not.
func (s *LoopScenario) eventAt(at time.Time) {
	s.restart()
	if s.looking || (!s.state.Looked.IsZero() && !at.After(s.state.Looked)) {
		at = laterOf(at, s.state.Looked).Add(time.Microsecond)
	}
	if at.After(s.state.Event) {
		s.state.Event = at
	}
}

func laterOf(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (s *LoopScenario) approvalList() []loom.ApprovalRecord {
	if len(s.approvals) == 0 {
		return nil
	}
	out := make([]loom.ApprovalRecord, 0, len(s.approvals))
	for _, r := range s.approvals {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b loom.ApprovalRecord) int { return a.AskedAt.Compare(b.AskedAt) })
	return out
}

// RunFor runs the clock d on from now: RunUntil(time.Now().Add(d)).
func (s *LoopScenario) RunFor(d time.Duration) *LoopScenario {
	s.t.Helper()
	return s.RunUntil(time.Now().Add(d))
}

// RunUntil runs the clock to until, making each look as it falls due by
// the runtime's rule (NextLook) and firing each timer at its time, in
// time order (a timer before a look due at the same instant): it sleeps
// to the next of them and makes it at time.Now(). It ends at until,
// with anything due at until made. Inside a synctest bubble the sleeps
// take no real time.
func (s *LoopScenario) RunUntil(until time.Time) *LoopScenario {
	s.t.Helper()
	for {
		wake, due := until, false
		if t, ok := s.timers.Next(); ok && !t.After(wake) {
			wake, due = t, true
		}
		if !s.done {
			if n := s.NextLook(); !n.After(wake) {
				wake, due = n, true
			}
		}
		sleepUntil(wake)
		if !due {
			return s
		}
		now := time.Now()
		s.timers.fireDue(now)
		if !s.done && !now.Before(s.NextLook()) {
			s.Look(now)
		}
	}
}

// Looks is every look the scenario has made, oldest first.
func (s *LoopScenario) Looks() []LookMade {
	return s.made
}

// Told is what the last look was told: why it was called.
func (s *LoopScenario) Told() loom.Looking {
	s.t.Helper()
	s.mustRun()
	return s.told
}

// Then asserts the commands the last look dispatched, in order, by JSON
// equality.
func (s *LoopScenario) Then(want ...loom.Command) *LoopScenario {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: look returned error: %v", s.err)
	}
	if len(s.cmds) != len(want) {
		s.t.Fatalf("loomtest: look dispatched %d commands, want %d: got %s", len(s.cmds), len(want), dumpCmds(s.cmds))
	}
	for i := range want {
		if !sameJSON(s.cmds[i], want[i]) {
			s.t.Fatalf("loomtest: command %d differs:\n got: %s\nwant: %s", i, js(s.cmds[i]), js(want[i]))
		}
	}
	return s
}

// ThenNothing asserts the last look dispatched nothing, and so emitted
// nothing: there was nothing to do.
func (s *LoopScenario) ThenNothing() *LoopScenario {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: look returned error: %v", s.err)
	}
	if len(s.cmds) != 0 || len(s.events) != 0 {
		s.t.Fatalf("loomtest: look dispatched %s and emitted %s, want nothing", dumpCmds(s.cmds), dump(s.events))
	}
	return s
}

// Emitted is the events the last look's commands emitted, in order.
func (s *LoopScenario) Emitted() []any {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: look returned error: %v", s.err)
	}
	return s.events
}

func (s *LoopScenario) mustRun() {
	s.t.Helper()
	if !s.ran {
		s.t.Fatal("loomtest: call Look first")
	}
}

func sleepUntil(at time.Time) {
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}

// --- agents ---

// Mesh offers the jobs of the agents the look asks to f, for tenant
// when an ask names none: f.Finish and f.Fail settle them before the
// next look, as Mesh's hook would, and the instance is looked at at
// once. With no Mesh the asks are only recorded (Asked, Agents). The
// scenario must name its instance (Instance, LoopOf) for its look to
// ask the agents its aggregate declares.
func (s *LoopScenario) Mesh(f *mesh.Fake, tenant string) *LoopScenario {
	s.mesh, s.tenant = f, tenant
	return s
}

// AgentAnswered records an agent's request as it stands — settled, with
// Mesh's outcome, when its Status is empty it is finished — for the next
// look to read with LookingAt(ctx).Agent. An open one holds its wait
// until its deadline.
func (s *LoopScenario) AgentAnswered(rec loom.AgentRecord) *LoopScenario {
	if s.agents == nil {
		s.agents = map[string]loom.AgentRecord{}
	}
	if rec.Status == "" {
		rec.Status = loom.AgentFinished
	}
	s.agents[rec.Name] = rec
	s.state, _ = s.state.WaitEnded(loom.AgentWait(rec.Name))
	if !rec.Settled() {
		s.state = s.state.Waited(loom.Wait{Name: loom.AgentWait(rec.Name), Owner: loom.AgentOwner, Deadline: rec.Deadline})
	}
	return s
}

// Agents is the instance's agent requests as they stand after the last
// look: what the next look reads.
func (s *LoopScenario) Agents() map[string]loom.AgentRecord {
	out := map[string]loom.AgentRecord{}
	for k, v := range s.agents {
		out[k] = v
	}
	return out
}

// Asked is the agents the last look asked, with what it asked them; an
// ask while the agent's request was open is among them, and changed
// nothing.
func (s *LoopScenario) Asked() []loom.AgentAsked {
	s.t.Helper()
	s.mustRun()
	return s.asks
}

func (s *LoopScenario) agentList() []loom.AgentRecord {
	if len(s.agents) == 0 {
		return nil
	}
	out := make([]loom.AgentRecord, 0, len(s.agents))
	for _, r := range s.agents {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b loom.AgentRecord) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// settleFromMesh settles each open request whose job the fake Mesh has
// ended, as its hook would: the wait ends and the instance is looked at
// at once.
func (s *LoopScenario) settleFromMesh(now time.Time) {
	if s.mesh == nil {
		return
	}
	for name, rec := range s.agents {
		if rec.Settled() || rec.JobID == "" {
			continue
		}
		o, ok := s.mesh.Ended(rec.JobID)
		if !ok {
			continue
		}
		rec.Status = loom.AgentFailed
		if o.End == mesh.EndFinished {
			rec.Status = loom.AgentFinished
		}
		at := now
		rec.EndedAt, rec.Outcome = &at, &o
		s.agents[name] = rec
		if !s.done {
			s.eventAt(now)
			s.state, _ = s.state.WaitEnded(loom.AgentWait(name))
		}
	}
}

// expireAgents expires each open request whose wait the look is told
// has run out, and cancels its job, as the runtime does before that
// look.
func (s *LoopScenario) expireAgents(expired []loom.Wait, now time.Time) {
	for _, w := range expired {
		name, ok := strings.CutPrefix(w.Name, "agent:")
		rec, held := s.agents[name]
		if !ok || !held || rec.Settled() || now.Before(rec.Deadline) {
			continue
		}
		at := now
		rec.Status, rec.EndedAt = loom.AgentExpired, &at
		s.agents[name] = rec
		if s.mesh != nil && rec.JobID != "" {
			_ = s.mesh.CancelJob(context.Background(), rec.JobID, "loom: the agent's deadline passed")
		}
	}
}

// openAgents records the asks the last look made, as the runtime does: a
// new request for an agent whose last is settled or that never asked,
// waited on until its deadline and its job offered to the fake Mesh;
// nothing for one still open.
func (s *LoopScenario) openAgents(now time.Time) error {
	for _, a := range s.asks {
		prior, had := s.agents[a.Def.Name]
		if had && !prior.Settled() {
			continue
		}
		rec := loom.AgentRecord{
			Aggregate: s.told.Aggregate, Namespace: s.told.Namespace, ID: s.told.ID, Name: a.Def.Name, Kind: a.Def.Kind,
			Attempt: prior.Attempt + 1, Ask: a.Ask, Status: loom.AgentRequested,
			Deadline: now.Add(a.Def.Deadline).UTC(), AskedAt: now,
		}
		if s.mesh != nil {
			o, err := rec.MeshOffer(s.reg.Service)
			if err != nil {
				return err
			}
			got, err := s.mesh.OfferJob(context.Background(), o)
			if err != nil {
				return err
			}
			rec.JobID = got.JobID
		}
		if s.agents == nil {
			s.agents = map[string]loom.AgentRecord{}
		}
		s.agents[rec.Name] = rec
		s.state = s.state.Waited(loom.Wait{Name: loom.AgentWait(rec.Name), Owner: loom.AgentOwner, Deadline: rec.Deadline})
	}
	return nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
