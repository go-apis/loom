package loom

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLoopActingBackoff(t *testing.T) {
	for acting, want := range map[int]time.Duration{
		0: LoopSoon, 1: LoopSoon, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second,
		6: 32 * time.Second, 7: LoopEvery, 8: LoopEvery, 1000: LoopEvery,
	} {
		if got := LoopActingBackoff(acting); got != want {
			t.Errorf("LoopActingBackoff(%d) = %s, want %s", acting, got, want)
		}
	}
}

// The events of a look's own commands are held back once its looks keep
// acting without progress; an event from outside never is.
func TestNextLookHoldsBackOnlyTheLoopsOwnEvents(t *testing.T) {
	looked := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	own := looked.Add(40 * time.Millisecond)
	for _, tc := range []struct {
		name  string
		state LoopState
		want  time.Duration // after looked
	}{
		{"one acting look: its event as ever", LoopState{Looked: looked, Event: own, Acting: 1}, 40*time.Millisecond + LoopSoon},
		{"two: 2s", LoopState{Looked: looked, Event: own, Acting: 2}, 2 * time.Second},
		{"five: 16s", LoopState{Looked: looked, Event: own, Acting: 5}, 16 * time.Second},
		{"never past the minute", LoopState{Looked: looked, Event: own, Acting: 40}, LoopEvery},
		{"an event from outside is not held", LoopState{Looked: looked, Event: own, Outside: own, Acting: 5}, 40*time.Millisecond + LoopSoon},
		{"an outside event before the look does not count", LoopState{Looked: looked, Event: own, Outside: looked.Add(-time.Second), Acting: 3}, 4 * time.Second},
		{"a wait's deadline is kept", LoopState{Looked: looked, Event: own, Acting: 6, Waits: []Wait{{Name: "w", Owner: "o", Deadline: looked.Add(3 * time.Second)}}}, 3 * time.Second},
		{"no event: the minute", LoopState{Looked: looked, Acting: 3}, LoopEvery},
	} {
		if got := tc.state.NextLook().Sub(looked); got != tc.want {
			t.Errorf("%s: next look %s after the last, want %s", tc.name, got, tc.want)
		}
	}
}

func TestActingAfter(t *testing.T) {
	looked := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	wait := Wait{Name: "deploy", Owner: "deployer", Deadline: looked.Add(time.Hour)}
	held := LoopState{Looked: looked, Event: looked.Add(time.Millisecond), Acting: 3, Sent: "poll", Waits: []Wait{wait}}
	same := LoopState{Waits: []Wait{wait}}
	sent := &lookSaid{sent: 1, sentAs: "poll"}
	later, owner := wait, wait
	later.Deadline = wait.Deadline.Add(time.Minute)
	owner.Owner = "operators"
	outside := held
	outside.Outside = looked.Add(time.Second)
	parked := same
	parked.Parked = true
	for _, tc := range []struct {
		name        string
		held, state LoopState
		said        *lookSaid
		err         error
		want        int
	}{
		{"sent the same again, nothing moved", held, same, sent, nil, 4},
		{"sent something else: a step, not a poll", held, same, &lookSaid{sent: 1, sentAs: "next step"}, nil, 1},
		{"sent nothing", held, same, &lookSaid{}, nil, 0},
		{"failed", held, same, sent, errTestLook, 0},
		{"first acting look", LoopState{Looked: looked}, LoopState{}, sent, nil, 1},
		{"an event from outside since the last look", outside, same, sent, nil, 1},
		{"a wait ended", held, LoopState{}, sent, nil, 1},
		{"a wait's deadline moved", held, LoopState{Waits: []Wait{later}}, sent, nil, 1},
		{"a wait's owner changed", held, LoopState{Waits: []Wait{owner}}, sent, nil, 1},
		{"a wait started", held, LoopState{Waits: []Wait{wait, {Name: "other", Owner: "o", Deadline: looked}}}, sent, nil, 1},
		{"parked", held, parked, sent, nil, 1},
		{"done", held, same, &lookSaid{sent: 1, sentAs: "poll", done: true}, nil, 1},
	} {
		if got := actingAfter(tc.held, tc.state, tc.said, tc.err); got != tc.want {
			t.Errorf("%s: acting %d, want %d", tc.name, got, tc.want)
		}
	}
}

type testLookError struct{}

func (testLookError) Error() string { return "the look failed" }

var errTestLook = testLookError{}

type testPoll struct {
	CommandBase
	Attempt int `json:"attempt"`
}

func (testPoll) LoomCommand() string { return "Poll" }

type testStep struct{ CommandBase }

func (testStep) LoomCommand() string { return "Step" }

// Two lists of commands stand for the same thing only when they hold
// the same types and payloads in the same order.
func TestCommandsDigest(t *testing.T) {
	b := CommandBase{Namespace: "ns", AggregateID: uuid.MustParse("11111111-2222-4333-8444-555555555555")}
	poll := func(n int) Command { return &testPoll{CommandBase: b, Attempt: n} }
	step := &testStep{CommandBase: b}
	if CommandsDigest(nil) != "" {
		t.Fatal("no commands stand for something")
	}
	same := CommandsDigest([]Command{poll(1), step})
	for name, other := range map[string][]Command{
		"another payload": {poll(2), step},
		"another type":    {step, step},
		"another order":   {step, poll(1)},
		"one fewer":       {poll(1)},
		"another target":  {poll(1), &testStep{CommandBase: CommandBase{Namespace: "ns", AggregateID: uuid.New()}}},
	} {
		if CommandsDigest(other) == same {
			t.Errorf("%s stands for the same commands", name)
		}
	}
	if CommandsDigest([]Command{poll(1), step}) != same {
		t.Error("the same commands stand for something else the second time")
	}
	// scheduled for another time, it is the same command
	if CommandsDigest([]Command{After(poll(1), time.Second)}) != CommandsDigest([]Command{After(poll(1), time.Hour)}) {
		t.Error("a command scheduled for another time stands for another command")
	}
}
