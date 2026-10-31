package loom

import (
	"testing"
	"time"
)

func TestStallBackoff(t *testing.T) {
	poll := time.Second
	for attempts, want := range map[int]time.Duration{
		0: 2 * time.Second, 1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second,
		8: 256 * time.Second, 9: maxStallBackoff, 1000: maxStallBackoff,
	} {
		if got := stallBackoff(poll, attempts); got != want {
			t.Errorf("stallBackoff(%v, %d) = %v, want %v", poll, attempts, got, want)
		}
	}
}

func TestLogLimiter(t *testing.T) {
	l := logLimiter{every: time.Minute}
	t0 := time.Unix(0, 0)
	if n, ok := l.allow(t0); !ok || n != 1 {
		t.Fatalf("first: %d %v", n, ok)
	}
	for i := 1; i <= 5; i++ {
		if _, ok := l.allow(t0.Add(time.Duration(i) * 10 * time.Second)); ok {
			t.Fatalf("let through %d within the minute", i)
		}
	}
	// the line that fires after a minute counts the five held back, and itself
	if n, ok := l.allow(t0.Add(time.Minute)); !ok || n != 6 {
		t.Fatalf("after a minute: %d %v", n, ok)
	}
}

func TestIdleWaitBacksOffOnlyWhileIdle(t *testing.T) {
	poll, max := 200*time.Millisecond, 5*time.Second
	if got := idleWait(poll, poll, 0, false); got != poll {
		t.Fatalf("no knob: an idle round sleeps poll, got %s", got)
	}
	if got := idleWait(poll, poll, poll/2, false); got != poll {
		t.Fatalf("a knob under poll: poll, got %s", got)
	}
	w := poll
	var seen []time.Duration
	for i := 0; i < 8; i++ {
		w = idleWait(w, poll, max, false)
		seen = append(seen, w)
	}
	want := []time.Duration{400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, max, max, max, max}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("idle rounds sleep %v, want %v", seen, want)
		}
	}
	if got := idleWait(max, poll, max, true); got != poll {
		t.Fatalf("a round with work goes back to poll, got %s", got)
	}
}
