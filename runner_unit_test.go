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
