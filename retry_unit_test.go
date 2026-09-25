package loom

import (
	"testing"
	"time"
)

// TestRetryBackoff proves each durable attempt waits within [Min, ceil],
// the ceiling doubling from Min per attempt and capped at MaxBackoff.
func TestRetryBackoff(t *testing.T) {
	r := &RetryPolicy{Max: 20, Min: 5 * time.Second, MaxBackoff: 5 * time.Minute}
	for n, ceil := range map[int]time.Duration{
		1: 5 * time.Second, 2: 10 * time.Second, 3: 20 * time.Second,
		7: 5 * time.Minute, 20: 5 * time.Minute, 1000: 5 * time.Minute,
	} {
		for range 200 {
			if got := r.backoff(n); got < r.Min || got > ceil {
				t.Fatalf("backoff(%d) = %v, want within [%v, %v]", n, got, r.Min, ceil)
			}
		}
	}
	fixed := &RetryPolicy{Max: 1, Min: time.Second, MaxBackoff: time.Second}
	if got := fixed.backoff(3); got != time.Second {
		t.Fatalf("min == max backoff = %v, want 1s", got)
	}
}

func TestRetryKey(t *testing.T) {
	evt := &Event{Service: "billing", GlobalSeq: 42}
	if got := retryKey("captureOnPaid", evt); got != "loom:retry:process:captureOnPaid/billing:42" {
		t.Fatalf("retryKey = %q", got)
	}
}
