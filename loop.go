package loom

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

// A loop's look is level-triggered: it reads how things stand and says
// what to do next, so it is safe to call again at any time and must be
// called again before long. When to call it is decided here, by a rule
// that reads no clock and no store — a function of what the runtime has
// noted since the last look — so it is the same rule under test and in
// the runner (ADR 0012).
//
// Three things bring a look forward:
//
//   - time: a loop never goes longer than LoopEvery without a look;
//   - an event: one that arrives since the last look brings the next to
//     within LoopSoon of it;
//   - a wait: a look that is waiting on someone is called when the
//     wait's deadline passes, and is told the wait has expired.
//
// One thing holds a look back. A look's own commands emit events on its
// instance, and those bring the next look to within LoopSoon like any
// other: a look that acts every time it is called, and changes nothing
// loom records about the instance, would be called flat out for as long
// as it kept acting (runsheet's item loop polled a deploy every 1.4s on
// 18 items for three hours, 2026-11-18). So once looks in a row have
// acted and left the instance's waits and outcome as they were, the
// events of the look's own commands bring the next look forward only to
// LoopActingBackoff: 2s, 4s, doubling to LoopEvery. An event the look
// did not cause is never held back, and it ends the run.

const (
	// LoopEvery is the longest a declared loop goes without a look.
	LoopEvery = time.Minute
	// LoopSoon is how soon after an event the loop is looked at.
	LoopSoon = time.Second
	// LoopBackoffFirst is how soon a look that failed is looked at
	// again. Each further failure in a row doubles it, up to LoopEvery.
	LoopBackoffFirst = time.Second
	// LoopFailureLimit is how many looks in a row may fail before the
	// instance is parked (Config.LoopFailureLimit sets another).
	LoopFailureLimit = 10
	// LoopParkFor is how long a parked instance waits on its operators
	// before it is looked at once more: nothing waits for ever.
	LoopParkFor = 24 * time.Hour
	// LoopParkedWait names the wait a parked instance holds, and
	// LoopParkedOwner who is to answer it: ending it (Client.EndWait)
	// looks at the instance again at once, its failures cleared.
	LoopParkedWait  = "loom.looks-failing"
	LoopParkedOwner = "operators"
)

// LoopActingFrom is how many looks in a row must act without progress
// before the next is held back: the second, since a look that acts once
// and is called again to see what came of it is how a loop is meant to
// move.
const LoopActingFrom = 2

// LoopActingBackoff is how soon after a look its own commands' events
// bring the next, once acting looks in a row have sent commands and
// changed nothing recorded about the instance: LoopSoon below
// LoopActingFrom, then 2s, doubling with each look after, never past
// LoopEvery.
func LoopActingBackoff(acting int) time.Duration {
	if acting < LoopActingFrom {
		return LoopSoon
	}
	d := 2 * time.Second
	for i := LoopActingFrom; i < acting && d < LoopEvery; i++ {
		d *= 2
	}
	return min(d, LoopEvery)
}

// LoopBackoff is the longest a loop goes without a look after failures
// failed looks in a row: LoopEvery for none, LoopBackoffFirst after the
// first, doubling with each one after, never past LoopEvery. A failure
// is most often passing — a refused command, a store blip — so the
// next look comes soon, and less soon each time it fails again.
func LoopBackoff(failures int) time.Duration {
	if failures <= 0 {
		return LoopEvery
	}
	d := LoopBackoffFirst
	for i := 1; i < failures && d < LoopEvery; i++ {
		d *= 2
	}
	return min(d, LoopEvery)
}

// Wait is something a loop is waiting on: an answer that is someone's to
// give, and the time after which the loop stops waiting for it.
type Wait struct {
	// Name says what is awaited, unique among the loop's waits.
	Name string `json:"name"`
	// Owner is who is to answer: a person, a service, another loop.
	Owner string `json:"owner"`
	// Deadline is when the wait has run out.
	Deadline time.Time `json:"deadline"`
}

// Expired reports whether the wait's deadline has passed at at.
func (w Wait) Expired(at time.Time) bool {
	return !w.Deadline.IsZero() && !at.Before(w.Deadline)
}

// LoopState is what the runtime has noted about one loop since it last
// looked.
type LoopState struct {
	// Looked is when the loop was last looked at; zero for never.
	Looked time.Time
	// Event is when the newest event the loop cares about arrived; zero
	// for none. One at or before Looked has been seen already.
	Event time.Time
	// Outside is when the newest event arrived that no look of the
	// loop's own caused: one from a command somebody else sent, an
	// answer, a child's note. Zero for none. The events a look's own
	// commands emit move Event and not Outside.
	Outside time.Time
	// Acting is how many looks in a row, up to the last one, sent
	// commands and left what loom records about the instance as it was:
	// its waits (name, owner and deadline) and its outcome (not failed,
	// not parked, not done), with no event from outside in between. Zero
	// once a look sends nothing or fails; one for a look that acts after
	// anything changed. From LoopActingFrom the next look is held back
	// (LoopActingBackoff).
	Acting int
	// Waits is what the loop is waiting on.
	Waits []Wait
	// Failures is how many looks in a row have failed, up to the last
	// one: zero once a look succeeds.
	Failures int
	// LastError is why the last look failed; empty once one succeeds.
	LastError string
	// Parked is whether the instance is parked: its looks failed
	// LoopFailureLimit times in a row, and it is looked at again only
	// for an event, its LoopParkedWait answered, or a deadline.
	Parked bool
}

// AfterLook is the state once a look made at at has ended, failed when
// err is not nil: a failure adds one to the failures in a row and is
// kept as LastError, and a look that did not fail ends the run and
// unparks the instance.
func (s LoopState) AfterLook(at time.Time, err error) LoopState {
	s.Looked = at
	if err != nil {
		s.Failures++
		s.LastError = err.Error()
	} else {
		s.Failures, s.LastError, s.Parked = 0, "", false
		s.Waits = withoutWait(s.Waits, LoopParkedWait)
	}
	return s
}

// ParkPast parks the instance once its failures in a row reach limit:
// it holds LoopParkedWait, owned by LoopParkedOwner, until LoopParkFor
// after its last look, and is looked at no sooner unless an event
// arrives or the wait is answered. A failure while parked waits again
// from that look. Below the limit it changes nothing.
func (s LoopState) ParkPast(limit int) LoopState {
	if limit <= 0 || s.Failures < limit {
		return s
	}
	s.Parked = true
	s.Waits = append(withoutWait(s.Waits, LoopParkedWait), Wait{
		Name:     LoopParkedWait,
		Owner:    LoopParkedOwner,
		Deadline: s.Looked.Add(LoopParkFor).UTC(),
	})
	return s
}

func withoutWait(waits []Wait, name string) []Wait {
	out := make([]Wait, 0, len(waits))
	for _, w := range waits {
		if w.Name != name {
			out = append(out, w)
		}
	}
	return out
}

// ActingWithoutProgress reports whether the loop's looks keep acting to
// no effect: LoopActingFrom or more in a row have sent commands and
// changed nothing loom records about the instance.
func (s LoopState) ActingWithoutProgress() bool { return s.Acting >= LoopActingFrom }

// NextLook is when the loop is to be looked at next: the earliest of
// LoopEvery after the last look (LoopBackoff of its failures, when the
// last looks failed), LoopSoon after an event that has arrived since
// (LoopActingBackoff after the look instead, while its looks keep acting
// without progress and only their own events have arrived), and the
// first deadline among its waits. A parked loop has no minute:
// only the event and the deadlines, LoopParkedWait's among them. A loop
// never looked at is due at once: the zero time.
func (s LoopState) NextLook() time.Time {
	if s.Looked.IsZero() {
		return time.Time{}
	}
	next := s.Looked.Add(LoopBackoff(s.Failures))
	if s.Parked {
		next = s.Looked.Add(LoopParkFor)
	}
	if s.Event.After(s.Looked) {
		soon := s.Event.Add(LoopSoon)
		if s.ActingWithoutProgress() && !s.Outside.After(s.Looked) {
			// nothing has arrived but what the look's own commands
			// emitted, and its looks keep acting to no effect: later
			// each time, never later than the minute
			soon = s.Looked.Add(LoopActingBackoff(s.Acting))
		}
		if soon.Before(next) {
			next = soon
		}
	}
	for _, w := range s.Waits {
		// a deadline that passed before the last look was told to it then
		if !w.Deadline.IsZero() && w.Deadline.After(s.Looked) && w.Deadline.Before(next) {
			next = w.Deadline
		}
	}
	return next
}

// Due reports whether the loop is to be looked at now.
func (s LoopState) Due(now time.Time) bool {
	return !now.Before(s.NextLook())
}

// Look is what a look at now is told: why it was called.
func (s LoopState) Look(now time.Time) Looking {
	l := Looking{At: now, Event: s.Event.After(s.Looked)}
	for _, w := range s.Waits {
		if w.Expired(now) {
			l.Expired = append(l.Expired, w)
		} else {
			l.Waiting = append(l.Waiting, w)
		}
	}
	return l
}

// Looking is what a look is told about why it was called. A look reads
// it with LookingAt; one that ignores it is still right, since a look
// decides from how things stand, and only slower to notice a wait has
// run out.
type Looking struct {
	// Aggregate, Namespace and ID name the instance looked at.
	Aggregate string
	Namespace string
	ID        uuid.UUID
	// At is the time of the look.
	At time.Time
	// Event is whether an event has arrived since the last look.
	Event bool
	// Expired is the waits whose deadline has passed: whoever was to
	// answer has not, and the loop decides what happens instead.
	Expired []Wait
	// Waiting is the waits still running.
	Waiting []Wait
	// Approvals is the instance's approvals as they stand: open, or
	// settled by an answer or by their deadline (approval.go).
	Approvals []ApprovalRecord
	// Children is the live children of a supervisor's instance, as
	// they stand (supervise.go); ChildNotes is what happened to its
	// children since its last look, each told once.
	Children   []Child
	ChildNotes []ChildNote
	// Agents is the instance's agent requests as they stand: open, or
	// settled by Mesh's outcome or by their deadline (agent.go).
	Agents []AgentRecord
}

// ExpiredWait is the expired wait of that name, if there is one.
func (l Looking) ExpiredWait(name string) (Wait, bool) {
	for _, w := range l.Expired {
		if w.Name == name {
			return w, true
		}
	}
	return Wait{}, false
}

type lookingKey struct{}

// WithLooking tells a look why it is called. The runtime sets it; a
// test harness sets it to call a look as the runtime would.
func WithLooking(ctx context.Context, l Looking) context.Context {
	return context.WithValue(ctx, lookingKey{}, l)
}

// LookingAt is what the look running under ctx was told. Outside a look
// it is the zero Looking.
func LookingAt(ctx context.Context) Looking {
	l, _ := ctx.Value(lookingKey{}).(Looking)
	return l
}

// lookSaid is what a look says beside the commands it returns.
type lookSaid struct {
	done  bool
	again time.Duration
	// sent is how many commands the look answered with (LookTurn.Sent)
	sent int
	// waits the look started, and the names of those it ended (wait.go)
	waits   []Wait
	stopped []string
	// approvals the look asked for (approval.go)
	approvals []Approval
	// children the look restarted (supervise.go)
	restarts []ChildRef
	// agents the look asked (agent.go), and what the runtime tells
	// AskAgent: the aggregate's declared agents and its Mesh
	agents     []agentAsked
	agentDefs  []AgentDef
	meshOn     bool
	meshTenant string
}

type lookSaidKey struct{}

// LookDone says the instance needs no more looks: it has reached an
// end, and nothing is left for its loop to do. The runtime stops
// looking at it. An event that arrives for it later starts the looks
// again. Outside a look run by the runtime it does nothing.
func LookDone(ctx context.Context) {
	if s, ok := ctx.Value(lookSaidKey{}).(*lookSaid); ok {
		s.done = true
	}
}

// LookAgain asks for the next look in d, sooner than the minute the
// runtime would otherwise leave: the loop knows something is due before
// then. It cannot ask for later than LoopEvery. Outside a look run by
// the runtime it does nothing.
func LookAgain(ctx context.Context, d time.Duration) {
	if s, ok := ctx.Value(lookSaidKey{}).(*lookSaid); ok && d > 0 && (s.again == 0 || d < s.again) {
		s.again = d
	}
}

// DeadlineDef is a deadline a @loop aggregate declares on its own state
// (`deadline <name> after <d> while <field> in (...)`): a wait loom keeps
// itself. The look that finds Holds true starts the wait, After from
// then; the look that finds it false ends it; when it expires the look is
// told once (ExpiredWait(DeadlineWait(name))), and it is not started
// again until Holds has been false.
type DeadlineDef struct {
	Name  string
	After time.Duration
	Holds func(AggregateState) bool
}

// DeadlineWait is the name of the wait a declared deadline keeps.
func DeadlineWait(name string) string { return deadlinePrefix + name }

const (
	deadlinePrefix = "deadline:"
	// DeadlineOwner owns the waits loom keeps for declared deadlines.
	DeadlineOwner = "loom"
)

// ValueIn reports whether v, a string or a string-kinded enum (or a
// pointer to one), is one of values. A nil pointer is in none. Generated
// code uses it for a deadline's `while <field> in (...)`.
func ValueIn(v any, values ...string) bool {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return false
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.String {
		return false
	}
	s := rv.String()
	for _, want := range values {
		if s == want {
			return true
		}
	}
	return false
}

// keepDeadlines is the change a look at an instance makes to the waits
// of its declared deadlines, before the look is told what it is told:
// each deadline that holds and has no wait (and has not fired since it
// last held) gets one, After from now; each that no longer holds loses
// its wait and is forgotten as fired. It answers the waits to start, the
// names to stop, and what has fired after it.
func keepDeadlines(defs []DeadlineDef, state AggregateState, waits []Wait, fired []string, now time.Time) (start []Wait, stop []string, stillFired []string) {
	held := map[string]bool{}
	for _, w := range waits {
		held[w.Name] = true
	}
	firedSet := map[string]bool{}
	for _, f := range fired {
		firedSet[f] = true
	}
	for _, d := range defs {
		name := DeadlineWait(d.Name)
		if d.Holds != nil && d.Holds(state) {
			if !held[name] && !firedSet[name] {
				start = append(start, Wait{Name: name, Owner: DeadlineOwner, Deadline: now.Add(d.After).UTC()})
			}
			continue
		}
		if held[name] {
			stop = append(stop, name)
		}
		delete(firedSet, name)
	}
	for f := range firedSet {
		stillFired = append(stillFired, f)
	}
	return start, stop, stillFired
}

// isDeadlineWait reports whether a wait is one loom keeps for a declared
// deadline.
func isDeadlineWait(name string) bool { return strings.HasPrefix(name, deadlinePrefix) }
