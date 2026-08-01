package loom

import "testing"

func fanEvent(seq int64, typ string) *Event {
	return &Event{GlobalSeq: seq, Type: typ}
}

func TestFanoutTypedWakes(t *testing.T) {
	f := &logFanout{}
	chat := f.register("projection:chat", []string{"ChatMessageSent"})
	places := f.register("projection:places", []string{"PlaceImported"})
	all := f.register("projection:audit", nil) // no declared events = wake always

	types := f.ingest([]*Event{fanEvent(1, "ChatMessageSent")})
	f.wake(types)
	if len(chat.ch) != 1 {
		t.Fatal("interested runner not woken")
	}
	if len(places.ch) != 0 {
		t.Fatal("uninterested runner woken")
	}
	if len(all.ch) != 1 {
		t.Fatal("wildcard runner not woken")
	}

	// unknown type set degrades to wake-everyone
	<-chat.ch
	<-all.ch
	f.wake(nil)
	if len(chat.ch) != 1 || len(places.ch) != 1 || len(all.ch) != 1 {
		t.Fatal("nil types must wake everyone")
	}
}

func TestFanoutTail(t *testing.T) {
	f := &logFanout{}
	f.setHead(10) // reader initialized at head=10; log holds 1..10

	// runner at head: buffer empty but covering — no events, no fallback
	if evts, ok := f.tail(10, 100); !ok || len(evts) != 0 {
		t.Fatalf("at-head runner should be covered: ok=%v n=%d", ok, len(evts))
	}
	// runner behind the reader start: must fall back to the log
	if _, ok := f.tail(4, 100); ok {
		t.Fatal("behind-head runner must fall back to direct reads")
	}

	f.ingest([]*Event{fanEvent(11, "A"), fanEvent(12, "B"), fanEvent(13, "A")})
	if evts, ok := f.tail(11, 100); !ok || len(evts) != 2 || evts[0].GlobalSeq != 12 {
		t.Fatalf("tail after 11: ok=%v n=%d", ok, len(evts))
	}
	if evts, ok := f.tail(10, 1); !ok || len(evts) != 1 || evts[0].GlobalSeq != 11 {
		t.Fatalf("tail limit: ok=%v n=%d", ok, len(evts))
	}
	// boundary: afterSeq just below buffer start is covered (nothing missing)
	if evts, ok := f.tail(13, 100); !ok || len(evts) != 0 {
		t.Fatalf("caught-up runner: ok=%v n=%d", ok, len(evts))
	}
}

func TestFanoutTrim(t *testing.T) {
	f := &logFanout{}
	evts := make([]*Event, fanoutBufCap+50)
	for i := range evts {
		evts[i] = fanEvent(int64(i+1), "T")
	}
	f.ingest(evts)
	if len(f.buf) != fanoutBufCap {
		t.Fatalf("buffer not trimmed: %d", len(f.buf))
	}
	// runner older than the trimmed window falls back
	if _, ok := f.tail(10, 100); ok {
		t.Fatal("pre-window runner must fall back to direct reads")
	}
	// runner inside the window is served
	if evts, ok := f.tail(int64(fanoutBufCap+40), 100); !ok || len(evts) != 10 {
		t.Fatalf("in-window tail: ok=%v n=%d", ok, len(evts))
	}
}
