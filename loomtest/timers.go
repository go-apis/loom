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
)

// --- timers ---

// TimerScenario holds the timers a test's reactions, looks and commands
// schedule (loom.At, loom.After, loom.WithKey, loom.CancelTimer) and
// fires each at its time through the registry's handlers, its events
// folded into the reader's state, as the timer runner would. A timer
// keyed like a pending one replaces it; a cancel deletes it.
//
// RunFor and RunUntil run the clock: they sleep to each timer and fire
// it at time.Now(). Inside a synctest bubble (synctest.Test) a timer a
// day or a year out fires at its time in no real time; loom.After reads
// the bubble's clock too, so "24h from now" is the bubble's now:
//
//	synctest.Test(t, func(t *testing.T) {
//		timers := loomtest.Timers(t, reg).Reading(reader)
//		timers.Take(loomtest.Reaction(t, reg, "expiry").When(ns, id, evt).Commands()...)
//		timers.RunFor(24 * time.Hour)
//		// timers.Fired() holds the expiry, fired at 24h
//	})
//
// Not here: the reactions the fired command's events start (a timer's
// command is handled; nothing downstream runs), leases, and parking a
// timer whose command is refused (FiredTimer.Err says it was).
type TimerScenario struct {
	t       *testing.T
	reg     *loom.Registry
	reader  *Reader
	pending []Timer
	fired   []FiredTimer
	// noted is told of events a command emitted, when a loop scenario
	// owns the timers
	noted func(agg *loom.AggregateDef, ns string, id uuid.UUID, at time.Time)
}

// Timer is a pending scheduled command.
type Timer struct {
	Key     string
	At      time.Time
	Command loom.Command
}

// FiredTimer is a timer that fired: when, what its command emitted, and
// why it was refused, when it was.
type FiredTimer struct {
	Timer
	FiredAt time.Time
	Events  []any
	Err     error
}

// Timers starts a scenario of scheduled commands.
func Timers(t *testing.T, reg *loom.Registry) *TimerScenario {
	return &TimerScenario{t: t, reg: reg, reader: &Reader{}}
}

// Reading injects the state the timers' commands are handled against.
func (s *TimerScenario) Reading(r *Reader) *TimerScenario {
	if r != nil {
		s.reader = r
	}
	return s
}

// Take holds the scheduled commands among cmds and applies the cancels,
// and answers the rest: what a reaction returned that is to run now.
func (s *TimerScenario) Take(cmds ...loom.Command) []loom.Command {
	var rest []loom.Command
	for _, cmd := range cmds {
		if !s.take(cmd) {
			rest = append(rest, cmd)
		}
	}
	return rest
}

func (s *TimerScenario) take(cmd loom.Command) bool {
	if inner, at, key, ok := loom.Scheduled(cmd); ok {
		s.cancel(key)
		s.pending = append(s.pending, Timer{Key: key, At: at, Command: inner})
		slices.SortStableFunc(s.pending, func(a, b Timer) int {
			if c := a.At.Compare(b.At); c != 0 {
				return c
			}
			return strings.Compare(a.Key, b.Key)
		})
		return true
	}
	if key, ok := loom.CancelsTimer(cmd); ok {
		s.cancel(key)
		return true
	}
	return false
}

func (s *TimerScenario) cancel(key string) {
	s.pending = slices.DeleteFunc(s.pending, func(t Timer) bool { return t.Key == key })
}

// Pending is the timers not yet fired, soonest first.
func (s *TimerScenario) Pending() []Timer {
	return slices.Clone(s.pending)
}

// Fired is the timers that have fired, in the order they did.
func (s *TimerScenario) Fired() []FiredTimer {
	return slices.Clone(s.fired)
}

// Next is when the soonest pending timer is due.
func (s *TimerScenario) Next() (time.Time, bool) {
	if len(s.pending) == 0 {
		return time.Time{}, false
	}
	return s.pending[0].At, true
}

// RunFor runs the clock d on from now: RunUntil(time.Now().Add(d)).
func (s *TimerScenario) RunFor(d time.Duration) *TimerScenario {
	return s.RunUntil(time.Now().Add(d))
}

// RunUntil sleeps to each pending timer due by until and fires it at
// time.Now(), soonest first, then sleeps to until.
func (s *TimerScenario) RunUntil(until time.Time) *TimerScenario {
	for {
		next, ok := s.Next()
		if !ok || next.After(until) {
			sleepUntil(until)
			return s
		}
		sleepUntil(next)
		s.fireDue(time.Now())
	}
}

// fireDue fires every timer due by now, soonest first.
func (s *TimerScenario) fireDue(now time.Time) {
	for len(s.pending) > 0 && !s.pending[0].At.After(now) {
		t := s.pending[0]
		s.pending = s.pending[1:]
		var (
			events []any
			err    error
		)
		if !s.take(t.Command) {
			events, err = s.apply(t.Command, now)
		}
		s.fired = append(s.fired, FiredTimer{Timer: t, FiredAt: now, Events: events, Err: err})
	}
}

// apply runs one command at at through its aggregate's handler against
// the reader's state and folds what it emits back in.
func (s *TimerScenario) apply(cmd loom.Command, at time.Time) ([]any, error) {
	ns, id := cmd.CommandTarget()
	ctx := loom.WithStateReader(context.Background(), s.reader)
	for _, agg := range s.reg.Aggregates {
		for _, def := range agg.Commands {
			if def.Name != cmd.LoomCommand() {
				continue
			}
			state := s.reader.Aggregates[key(agg.Name, ns, id)]
			if state == nil {
				state = agg.NewState()
			}
			events, err := def.Handle(ctx, state, cmd)
			if err != nil {
				return nil, err
			}
			for _, evt := range events {
				name := evt.(loom.DomainEvent).LoomEvent()
				if !containsString(def.Emits, name) {
					return nil, fmt.Errorf("emitted undeclared event %s", name)
				}
				if err := state.Fold(name, evt); err != nil {
					return nil, err
				}
			}
			s.reader.AddAggregate(agg.Name, ns, id, state)
			if len(events) > 0 && s.noted != nil {
				s.noted(agg, ns, id, at)
			}
			return events, nil
		}
	}
	return nil, fmt.Errorf("loomtest: no aggregate handles %s", cmd.LoomCommand())
}

func containsString(list []string, s string) bool {
	return slices.Contains(list, s)
}
