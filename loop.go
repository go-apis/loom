package loom

import (
	"context"
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

const (
	// LoopEvery is the longest a declared loop goes without a look.
	LoopEvery = time.Minute
	// LoopSoon is how soon after an event the loop is looked at.
	LoopSoon = time.Second
)

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
	// Waits is what the loop is waiting on.
	Waits []Wait
}

// NextLook is when the loop is to be looked at next: the earliest of
// LoopEvery after the last look, LoopSoon after an event that has
// arrived since, and the first deadline among its waits. A loop never
// looked at is due at once: the zero time.
func (s LoopState) NextLook() time.Time {
	if s.Looked.IsZero() {
		return time.Time{}
	}
	next := s.Looked.Add(LoopEvery)
	if s.Event.After(s.Looked) {
		if soon := s.Event.Add(LoopSoon); soon.Before(next) {
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
