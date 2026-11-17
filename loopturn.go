package loom

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// A LookTurn is one look at one instance of a @loop aggregate as the
// runtime makes it, less the store: what the declared deadlines start
// and end, what the look is told, what it says while it runs, and, when
// it ends, how the instance stands and when it is next looked at. None
// of it reads a clock or a store — the time of the look is given — so
// the runner makes every look through one, and a test harness
// (loomtest) drives the same turns through a synctest bubble's clock.
//
// What a turn leaves out is the store's: claiming the instance, loading
// its state and its approvals, dispatching the look's commands, writing
// the row back. Those stay in the runner (looprun.go).
type LookTurn struct {
	// At is the instant the look is made: the look is of that instant.
	At time.Time
	// Told is what the look is told. The runner adds the instance's
	// name, its approvals and its children before calling the look.
	Told Looking
	// Fired is the declared deadlines that have expired and held since,
	// as the look leaves them.
	Fired []string

	prior []string // Fired before the look, kept when it fails
	said  *lookSaid
	ended bool
}

// StartLook begins a look made at at, on an instance whose loop state is
// s and whose declared deadlines in fired have expired and held since.
// The declared deadlines (defs, over the instance's state agg; none for
// a loop that declares none) start and end their waits first, as the
// state stands (DeadlineDef), and the look is then told which waits have
// expired by at (LoopState.Look).
func StartLook(s LoopState, fired []string, defs []DeadlineDef, agg AggregateState, at time.Time) *LookTurn {
	t := &LookTurn{At: at, Fired: fired, prior: fired, said: &lookSaid{}}
	if len(defs) > 0 {
		begin, stop, still := keepDeadlines(defs, agg, s.Waits, fired, at)
		t.said.waits, t.said.stopped, t.Fired = append(t.said.waits, begin...), append(t.said.stopped, stop...), still
		kept := s.Waits[:0:0]
		for _, w := range s.Waits {
			if !contains(stop, w.Name) {
				kept = append(kept, w)
			}
		}
		s.Waits = append(kept, begin...)
	}
	t.Told = s.Look(at)
	for _, w := range t.Told.Expired {
		if isDeadlineWait(w.Name) && !contains(t.Fired, w.Name) {
			t.Fired = append(t.Fired, w.Name)
		}
	}
	return t
}

// Context is ctx carrying what the look is told (LookingAt) and taking
// what it says: LookDone, LookAgain, WaitFor, StopWaiting, AskApproval
// and RestartChild.
func (t *LookTurn) Context(ctx context.Context) context.Context {
	return context.WithValue(WithLooking(ctx, t.Told), lookSaidKey{}, t.said)
}

// settle notes how the look came out: a look that failed started and
// ended nothing, so what it said is dropped with it, and the waits it
// was told had expired are told again.
func (t *LookTurn) settle(err error) {
	if t.ended {
		return
	}
	t.ended = true
	if err != nil {
		t.Told, t.said, t.Fired = Looking{}, &lookSaid{}, t.prior
	}
}

// End notes the look ended, failed when err is not nil, on an instance
// that holds held now — its waits, its failures in a row, whether it is
// parked, and its newest event, as they stand once the look is over (a
// wait started from outside while it ran is among them). It answers the
// instance's state after the look and when it is next looked at: the
// waits settled by what the look was told and said, the look noted
// (LoopState.AfterLook, then ParkPast limit), and the next look by
// LoopState.NextLook, or sooner when the look asked for it (LookAgain).
func (t *LookTurn) End(held LoopState, limit int, err error) (LoopState, time.Time) {
	t.settle(err)
	return lookEnded(held, t.At, t.Told, t.said, true, err, limit)
}

// Done reports whether a look that ended without failing said LookDone.
func (t *LookTurn) Done() bool { return t.ended && t.said.done }

// Approvals is what the look asked for (AskApproval), once it ended;
// none when it failed.
func (t *LookTurn) Approvals() []Approval { return t.said.approvals }

// Restarts is the children the look restarted (RestartChild), once it
// ended; none when it failed.
func (t *LookTurn) Restarts() []ChildRef { return t.said.restarts }

// lookEnded is the state of an instance once a look made at start ended,
// from the state held as it stands now: the waits settled by what the
// look was told and said, the outcome noted when the look was made
// (AfterLook, then ParkPast limit), and the next look. A look not made
// (no loop in this build) leaves the failures as they stand.
func lookEnded(held LoopState, start time.Time, told Looking, said *lookSaid, made bool, lookErr error, limit int) (LoopState, time.Time) {
	state := LoopState{
		Looked:    start,
		Event:     held.Event,
		Waits:     settleWaits(held.Waits, told, said),
		Failures:  held.Failures,
		LastError: held.LastError,
		Parked:    held.Parked,
	}
	if made {
		state = state.AfterLook(start, lookErr).ParkPast(limit)
	}
	next := state.NextLook()
	if said != nil && said.again > 0 {
		if asked := start.Add(said.again); asked.Before(next) {
			next = asked
		}
	}
	return state, next
}

// Waited is the state once a wait is started from outside a look
// (Client.Wait): it is added, or replaces its namesake.
func (s LoopState) Waited(w Wait) LoopState {
	w.Deadline = w.Deadline.UTC()
	s.Waits = settleWaits(s.Waits, Looking{}, &lookSaid{waits: []Wait{w}})
	return s
}

// WaitEnded is the state once the wait of that name is ended from
// outside a look (Client.EndWait): it has been answered. A parked
// instance left without its LoopParkedWait is unparked, its failures
// cleared, and is to be looked at at once: now reports that.
func (s LoopState) WaitEnded(name string) (out LoopState, now bool) {
	s.Waits = settleWaits(s.Waits, Looking{}, &lookSaid{stopped: []string{name}})
	if s.Parked && !holdsWait(s.Waits, LoopParkedWait) {
		s.Parked, s.Failures = false, 0
		return s, true
	}
	return s, false
}

// ApprovalWait is the name of the wait an approval of that name keeps on
// its instance.
func ApprovalWait(name string) string { return approvalWait(name) }

// approvalOfWait is the name of the approval a wait is kept for.
func approvalOfWait(wait string) (string, bool) { return strings.CutPrefix(wait, "approval:") }

// PastDeadline is the approval as its deadline leaves it at at: one still
// open whose deadline has passed takes its declared deadline action
// (approved, rejected or expired), answered by ApprovalByDeadline at at.
// changed is false when the deadline changes nothing.
func (r ApprovalRecord) PastDeadline(at time.Time) (out ApprovalRecord, changed bool) {
	if r.Settled() || at.Before(r.Deadline) {
		return r, false
	}
	switch r.Approval.AtDeadline {
	case AtDeadlineApprove:
		r.Status = ApprovalApproved
	case AtDeadlineReject:
		r.Status = ApprovalRejected
	default:
		r.Status = ApprovalExpired
	}
	r.AnsweredBy, r.Reason, r.AnsweredAt = ApprovalByDeadline, "", &at
	return r, true
}

// Answer is the approval once actor answers it at at. An approval
// already settled, or past its deadline, is refused: the deadline's
// action stands; so is an answerer who does not hold its role
// (ErrNotYourApproval).
func (r ApprovalRecord) Answer(actor string, answer ApprovalAnswer, at time.Time) (ApprovalRecord, error) {
	if r.Settled() {
		return r, fmt.Errorf("loom: approval %q is %s already, by %s", r.Name, r.Status, r.AnsweredBy)
	}
	if !at.Before(r.Deadline) {
		return r, fmt.Errorf("loom: approval %q passed its deadline at %s: what it declared for the deadline stands", r.Name, r.Deadline.Format(time.RFC3339))
	}
	if !contains(answer.Roles, r.Role) {
		return r, fmt.Errorf("%w (%s)", ErrNotYourApproval, r.Role)
	}
	r.Status = ApprovalRejected
	if answer.Approve {
		r.Status = ApprovalApproved
	}
	r.AnsweredBy, r.Reason, r.AnsweredAt = actor, strings.TrimSpace(answer.Reason), &at
	return r, nil
}

// Scheduled reports what a command At or After returned holds: the
// command to dispatch, when, and the timer's key (WithKey, or the
// command's type and target). ok is false for any other command.
func Scheduled(cmd Command) (inner Command, at time.Time, key string, ok bool) {
	sc, ok := cmd.(*scheduledCommand)
	if !ok {
		return nil, time.Time{}, "", false
	}
	return sc.Command, sc.at, timerKey(sc.Command, sc.key), true
}

// CancelsTimer reports the key of the timer a CancelTimer command
// deletes. ok is false for any other command.
func CancelsTimer(cmd Command) (key string, ok bool) {
	ct, ok := cmd.(*cancelTimer)
	if !ok {
		return "", false
	}
	return timerKey(ct.Command, ct.key), true
}
