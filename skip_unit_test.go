package loom

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// walkRig is a process runner without a store: a log, a checkpoint, and
// counts of what the walk wrote and delivered.
type walkRig struct {
	log        []*Event
	checkpoint int64
	writes     int
	handled    []int64
	leading    func() bool
	handleErr  func(seq int64) error
	saveErr    func(seq int64) error
	// moved, when set, moves the checkpoint from under the next pass, as
	// another leader or a reset would
	moved int64
}

var walkSubscribed = []string{"Mine"}

func walkLog(types ...string) []*Event {
	out := make([]*Event, len(types))
	for i, typ := range types {
		out[i] = &Event{GlobalSeq: int64(i + 1), Type: typ}
	}
	return out
}

func repeatType(typ string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = typ
	}
	return out
}

func (r *walkRig) read(after int64, limit int) []*Event {
	var out []*Event
	for _, evt := range r.log {
		if evt.GlobalSeq > after && len(out) < limit {
			out = append(out, evt)
		}
	}
	return out
}

// step is processStep's shape: read the checkpoint, read a batch, walk it.
func (r *walkRig) step(ctx context.Context) (int, error) {
	seq := r.checkpoint
	events := r.read(seq, logBatch)
	if len(events) == 0 {
		return 0, nil
	}
	leading := r.leading
	if leading == nil {
		leading = func() bool { return true }
	}
	return processWalk{
		subscribed: walkSubscribed,
		leading:    leading,
		handle: func(_ context.Context, evt *Event) error {
			if r.handleErr != nil {
				if err := r.handleErr(evt.GlobalSeq); err != nil {
					return err
				}
			}
			r.handled = append(r.handled, evt.GlobalSeq)
			return nil
		},
		save: func(_ context.Context, seq int64) (bool, error) {
			if r.saveErr != nil {
				if err := r.saveErr(seq); err != nil {
					return false, err
				}
			}
			r.writes++
			r.checkpoint = seq
			return true, nil
		},
		pass: func(_ context.Context, from, to int64) (bool, error) {
			if r.moved != 0 {
				r.checkpoint, r.moved = r.moved, 0
			}
			if r.checkpoint != from {
				return false, nil
			}
			r.writes++
			r.checkpoint = to
			return true, nil
		},
		ahead: func(_ context.Context, after int64) (int64, error) {
			to := after
			for _, evt := range r.read(after, ignoredScan) {
				if contains(walkSubscribed, evt.Type) {
					break
				}
				to = evt.GlobalSeq
			}
			return to, nil
		},
	}.walk(ctx, seq, events, len(events) == logBatch)
}

// drain steps until a step is idle, as runLogLoop does.
func (r *walkRig) drain(t *testing.T) {
	t.Helper()
	for i := 0; ; i++ {
		n, err := r.step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if i > len(r.log)+10 {
			t.Fatal("the runner never went idle")
		}
	}
}

func TestIgnoredRun(t *testing.T) {
	for _, tc := range []struct {
		types []string
		want  int
	}{
		{nil, 0},
		{[]string{"Mine"}, 0},
		{[]string{"Other", "Other", "Mine", "Other"}, 2},
		{[]string{"Other", "Other"}, 2},
	} {
		if got := ignoredRun(walkLog(tc.types...), walkSubscribed); got != tc.want {
			t.Errorf("ignoredRun(%v) = %d, want %d", tc.types, got, tc.want)
		}
	}
}

// The incident's shape: a long run of somebody else's events, then one
// of the process's own. The run costs a write or two, not one an event.
func TestWalkPassesALongIgnoredRunInFewWrites(t *testing.T) {
	const ignored = 10_000
	r := &walkRig{log: walkLog(append(repeatType("Other", ignored), "Mine")...)}
	r.drain(t)
	if !slices.Equal(r.handled, []int64{ignored + 1}) {
		t.Fatalf("handled %v, want the one subscribed event", r.handled)
	}
	if r.checkpoint != ignored+1 {
		t.Fatalf("checkpoint at %d, want the log's end %d", r.checkpoint, ignored+1)
	}
	if r.writes > 3 {
		t.Fatalf("%d checkpoint writes for %d ignored events and one subscribed, want at most 3", r.writes, ignored)
	}
}

// A run longer than one look ahead costs one write per look, and the
// checkpoint still ends at the head.
func TestWalkPassesARunLongerThanOneLook(t *testing.T) {
	const ignored = 3*ignoredScan + 17
	r := &walkRig{log: walkLog(repeatType("Other", ignored)...)}
	r.drain(t)
	if r.checkpoint != ignored {
		t.Fatalf("checkpoint at %d, want %d", r.checkpoint, ignored)
	}
	if r.writes > 5 {
		t.Fatalf("%d checkpoint writes for %d ignored events, want at most 5", r.writes, ignored)
	}
}

// Subscribed events are delivered once each, in order, whatever lies
// between them, and never with more writes than the old one-per-event.
func TestWalkDeliversSubscribedEventsInOrder(t *testing.T) {
	var types []string
	var want []int64
	for i := 0; i < 1500; i++ {
		typ := "Other"
		if i%7 == 0 || i%200 == 199 || i == 600 || i == 601 {
			typ = "Mine"
			want = append(want, int64(i+1))
		}
		types = append(types, typ)
	}
	r := &walkRig{log: walkLog(types...)}
	r.drain(t)
	if !slices.Equal(r.handled, want) {
		t.Fatalf("handled %v\nwant %v", r.handled, want)
	}
	if r.checkpoint != int64(len(types)) {
		t.Fatalf("checkpoint at %d, want %d", r.checkpoint, len(types))
	}
	if r.writes > len(types) {
		t.Fatalf("%d writes for %d events: more than one an event", r.writes, len(types))
	}
}

// A reaction that fails outright (the process crashing mid-reaction is
// the same to the store) leaves the checkpoint just short of its event:
// the next run starts there, redelivers nothing handled before it and
// skips nothing after it.
func TestWalkCrashMidRunRedeliversNothingHandled(t *testing.T) {
	types := append([]string{"Mine"}, repeatType("Other", 450)...)
	types = append(types, "Mine") // 452
	types = append(types, repeatType("Other", 300)...)
	types = append(types, "Mine") // 753
	crash := errors.New("crashed")
	r := &walkRig{log: walkLog(types...)}
	r.handleErr = func(seq int64) error {
		if seq == 452 {
			return crash
		}
		return nil
	}
	var err error
	for err == nil {
		_, err = r.step(context.Background())
	}
	if !errors.Is(err, crash) {
		t.Fatal(err)
	}
	if !slices.Equal(r.handled, []int64{1}) {
		t.Fatalf("handled %v before the crash, want [1]", r.handled)
	}
	if r.checkpoint != 451 {
		t.Fatalf("checkpoint at %d after the crash, want 451: just short of the event that was not handled", r.checkpoint)
	}
	// a new runner, same store
	r.handleErr = nil
	r.drain(t)
	if !slices.Equal(r.handled, []int64{1, 452, 753}) {
		t.Fatalf("handled %v across the crash, want [1 452 753]", r.handled)
	}
}

// A crash between a reaction and its checkpoint write redelivers that
// one event, as it always has (at-least-once), and nothing before it.
func TestWalkCrashBeforeTheWriteRedeliversOnlyThatEvent(t *testing.T) {
	r := &walkRig{log: walkLog("Mine", "Other", "Other", "Mine", "Other", "Mine")}
	crash := errors.New("crashed")
	r.saveErr = func(seq int64) error {
		if seq == 4 {
			return crash
		}
		return nil
	}
	if _, err := r.step(context.Background()); !errors.Is(err, crash) {
		t.Fatalf("step: %v", err)
	}
	if r.checkpoint != 3 {
		t.Fatalf("checkpoint at %d, want 3", r.checkpoint)
	}
	r.saveErr = nil
	r.drain(t)
	if !slices.Equal(r.handled, []int64{1, 4, 4, 6}) {
		t.Fatalf("handled %v, want [1 4 4 6]", r.handled)
	}
}

// A leader that loses the lease mid-batch stops between events and
// writes nothing more, not even for the ignored events it had read.
func TestWalkStopsWhenTheLeaseGoes(t *testing.T) {
	r := &walkRig{log: walkLog("Mine", "Other", "Other", "Mine")}
	r.leading = func() bool { return false } // gone after the first event
	n, err := r.step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || r.checkpoint != 1 || r.writes != 1 || !slices.Equal(r.handled, []int64{1}) {
		t.Fatalf("got through %d, checkpoint %d, writes %d, handled %v: want one event and one write", n, r.checkpoint, r.writes, r.handled)
	}
}

// Two runners of one process: when another has moved the checkpoint
// (a new leader further on, an operator's reset further back), a pass
// leaves it alone and the step ends without delivering what follows
// from its own stale read.
func TestWalkLeavesACheckpointSomeoneElseMoved(t *testing.T) {
	for _, movedTo := range []int64{1, 6} {
		r := &walkRig{log: walkLog("Mine", "Other", "Other", "Mine", "Other", "Mine"), checkpoint: 1}
		r.moved = movedTo
		if movedTo == 1 {
			// a reset back to before this step's read: start the rig a
			// step further on so 1 is behind it
			r.checkpoint = 4
			r.handled = nil
		}
		n, err := r.step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatalf("moved to %d: the step reported idle; it must step again from the new checkpoint", movedTo)
		}
		if r.checkpoint != movedTo {
			t.Fatalf("moved to %d: the pass overwrote it with %d", movedTo, r.checkpoint)
		}
		if len(r.handled) != 0 || r.writes != 0 {
			t.Fatalf("moved to %d: handled %v with %d writes after losing its place", movedTo, r.handled, r.writes)
		}
	}
}
