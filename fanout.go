package loom

import "sync"

// fanoutBufCap bounds the in-memory tail of the event log shared by all
// runners. Runners further behind than the buffer (catch-up after a skip
// streak, rebuilds) fall back to reading the log directly.
const fanoutBufCap = 1024

// runnerWake is one runner's registration with the fan-out reader: a
// cap-1 wake channel plus the event types it subscribes to (nil = all).
type runnerWake struct {
	name     string
	ch       chan struct{}
	interest map[string]bool
}

// wants reports whether any of the just-ingested types concern this
// runner. An unknown type set (nil/empty) wakes everyone — degraded to
// the old behavior, never worse.
func (rw *runnerWake) wants(types map[string]bool) bool {
	if rw.interest == nil || len(types) == 0 {
		return true
	}
	for t := range types {
		if rw.interest[t] {
			return true
		}
	}
	return false
}

// logFanout is the per-instance shared log tail. One reader goroutine
// (Client.runReader) ingests new events exactly once — instead of every
// runner re-reading and re-decrypting the same slice — and wakes exactly
// the runners whose subscriptions match, instead of the old single
// shared nudge that woke one arbitrary runner and left the rest to
// their poll tick.
type logFanout struct {
	mu      sync.Mutex
	buf     []*Event // ascending global_seq; decrypted; treated read-only
	head    int64    // highest seq the reader has ingested
	runners []*runnerWake
}

func (f *logFanout) register(name string, events []string) *runnerWake {
	rw := &runnerWake{name: name, ch: make(chan struct{}, 1)}
	if len(events) > 0 {
		rw.interest = make(map[string]bool, len(events))
		for _, e := range events {
			rw.interest[e] = true
		}
	}
	f.mu.Lock()
	f.runners = append(f.runners, rw)
	f.mu.Unlock()
	return rw
}

func (f *logFanout) headSeq() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.head
}

func (f *logFanout) setHead(seq int64) {
	f.mu.Lock()
	f.head = seq
	f.mu.Unlock()
}

// ingest appends freshly-read events and returns their distinct types.
func (f *logFanout) ingest(events []*Event) map[string]bool {
	types := make(map[string]bool)
	f.mu.Lock()
	for _, e := range events {
		f.buf = append(f.buf, e)
		f.head = e.GlobalSeq
		types[e.Type] = true
	}
	if n := len(f.buf) - fanoutBufCap; n > 0 {
		// reallocate so the trimmed prefix is actually collectable
		f.buf = append(f.buf[:0:0], f.buf[n:]...)
	}
	f.mu.Unlock()
	return types
}

// wake nudges every runner interested in the given types (nil = all).
func (f *logFanout) wake(types map[string]bool) {
	f.mu.Lock()
	runners := f.runners
	f.mu.Unlock()
	for _, rw := range runners {
		if rw.wants(types) {
			select {
			case rw.ch <- struct{}{}:
			default:
			}
		}
	}
}

func (f *logFanout) wakeAll() { f.wake(nil) }

// flush drops the buffered tail without moving head. Runners behind head
// fall back to direct log reads, which decrypt afresh. Required whenever
// the effective content of already-ingested events changes — shredding a
// stream's key redacts its PII, and buffered pre-shred plaintext must
// not out-live it (folds and rebuilds would resurrect it).
func (f *logFanout) flush() {
	f.mu.Lock()
	f.buf = nil
	f.mu.Unlock()
}

// tail returns buffered events with seq > afterSeq (up to limit) and
// whether the buffer covers afterSeq — false means the caller is too far
// behind and must read the log directly.
func (f *logFanout) tail(afterSeq int64, limit int) ([]*Event, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.buf) == 0 {
		// an empty buffer covers exactly the reader head: a runner at
		// head has nothing to read; anyone behind goes to the log.
		return nil, afterSeq >= f.head
	}
	if afterSeq < f.buf[0].GlobalSeq-1 {
		return nil, false
	}
	lo, hi := 0, len(f.buf)
	for lo < hi {
		mid := (lo + hi) / 2
		if f.buf[mid].GlobalSeq <= afterSeq {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	out := f.buf[lo:]
	if len(out) > limit {
		out = out[:limit]
	}
	return out, true
}
