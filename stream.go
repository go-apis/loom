package loom

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Streaming is one-directional by design (server → client) and rides plain
// HTTP as SSE: browser-native EventSource, proxy/Cloud-Run/Workers
// friendly. The global sequence makes every stream resumable — the SSE id
// is the sequence, and EventSource's automatic Last-Event-ID reconnect
// picks up exactly where it left off.

// Watch registers a wake-up channel signalled whenever this service's
// log may have advanced — local dispatches and, via LISTEN, other
// instances'. The signal is a coalesced hint with no payload: reload
// and diff. cancel unregisters. This is the primitive behind the SSE
// endpoints and the gateway's subscriptions.
func (c *Client) Watch() (<-chan struct{}, func()) {
	return c.watchLog()
}

// watchLog registers a wake-up channel signalled whenever this service's
// log may have advanced (local dispatches and, via LISTEN, other
// instances').
func (c *Client) watchLog() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.watchMu.Lock()
	c.watchers[ch] = true
	c.watchMu.Unlock()
	return ch, func() {
		c.watchMu.Lock()
		delete(c.watchers, ch)
		c.watchMu.Unlock()
	}
}

func (c *Client) broadcastLog() {
	select {
	case c.nudge <- struct{}{}:
	default:
	}
	c.watchMu.Lock()
	for ch := range c.watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	c.watchMu.Unlock()
}

// listenLoop subscribes to the service's pg_notify channel so runners and
// SSE watchers on THIS instance wake for OTHER instances' writes.
func (c *Client) listenLoop(ctx context.Context) {
	channel := "loom_" + c.reg.Service
	for ctx.Err() == nil {
		if err := c.listenOnce(ctx, channel); err != nil && ctx.Err() == nil {
			c.log.WarnContext(ctx, "listen loop reconnecting", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (c *Client) listenOnce(ctx context.Context, channel string) error {
	conn, err := c.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+pgIdent(channel)); err != nil {
		return err
	}
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		// shred notifications evict cached data keys on every instance
		if rest, ok := strings.CutPrefix(n.Payload, "shred:"); ok {
			if ns, id, ok := strings.Cut(rest, ":"); ok {
				c.dropDEK(ns, id)
			}
			continue
		}
		c.broadcastLog()
	}
}

func pgIdent(s string) string {
	return `"` + s + `"`
}

// --- SSE plumbing ---

const streamHeartbeat = 15 * time.Second

type sseWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

func newSSE(w http.ResponseWriter) (*sseWriter, bool) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, false
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	f.Flush()
	return &sseWriter{w: w, f: f}, true
}

func (s *sseWriter) event(id, event string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if id != "" {
		fmt.Fprintf(s.w, "id: %s\n", id)
	}
	if event != "" {
		fmt.Fprintf(s.w, "event: %s\n", event)
	}
	fmt.Fprintf(s.w, "data: %s\n\n", raw)
	s.f.Flush()
	return nil
}

func (s *sseWriter) ping() {
	fmt.Fprint(s.w, ": ping\n\n")
	s.f.Flush()
}

// streamLoop runs the shared wait/step cycle: step on every log wake-up,
// heartbeat on the ticker, exit with the request.
func (c *Client) streamLoop(ctx context.Context, sse *sseWriter, step func() error) {
	wake, cancel := c.watchLog()
	defer cancel()
	heartbeat := time.NewTicker(streamHeartbeat)
	defer heartbeat.Stop()
	// poll fallback: LISTEN covers other instances, but belt-and-braces
	poll := time.NewTicker(2 * time.Second)
	defer poll.Stop()

	if err := step(); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-poll.C:
		case <-heartbeat.C:
			sse.ping()
			continue
		}
		if err := step(); err != nil {
			return
		}
	}
}

// GET /events/stream — live log tail. Starts after ?after_seq, the
// Last-Event-ID reconnect header, or "now". Same filters as /events.
func (c *Client) apiEventsStream(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Query()
	q := LogQuery{
		Namespace:     p.Get("namespace"),
		Type:          p.Get("type"),
		AggregateType: p.Get("aggregate_type"),
		AggregateID:   p.Get("aggregate_id"),
		CorrelationID: p.Get("correlation_id"),
		Ascending:     true,
		Limit:         500,
	}
	cursor := int64(-1)
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		cursor, _ = strconv.ParseInt(v, 10, 64)
	} else if v := p.Get("after_seq"); v != "" {
		cursor, _ = strconv.ParseInt(v, 10, 64)
	}
	if cursor < 0 {
		// default: only new events from here on
		if err := c.db.QueryRow(r.Context(), `SELECT coalesce(max(global_seq), 0) FROM loom_events WHERE service=$1`, c.reg.Service).Scan(&cursor); err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	sse, ok := newSSE(w)
	if !ok {
		apiError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	c.streamLoop(r.Context(), sse, func() error {
		for {
			q.AfterSeq = cursor
			entries, err := c.QueryLog(r.Context(), q)
			if err != nil || len(entries) == 0 {
				return err
			}
			for _, e := range entries {
				if err := sse.event(strconv.FormatInt(e.GlobalSeq, 10), e.Type, e); err != nil {
					return err
				}
				cursor = e.GlobalSeq
			}
			if len(entries) < q.Limit {
				return nil
			}
		}
	})
}

// GET /entities/{name}/{id}/stream — read-model watch: the current row
// immediately, then a fresh copy on every change.
func (c *Client) apiEntityStream(w http.ResponseWriter, r *http.Request) {
	c.docStream(w, r, func(ns string, id uuid.UUID) (any, error) {
		return c.Entity(r.Context(), r.PathValue("name"), ns, id)
	})
}

// GET /aggregates/{name}/{id}/stream — folded state + version on change.
func (c *Client) apiAggregateStream(w http.ResponseWriter, r *http.Request) {
	c.docStream(w, r, func(ns string, id uuid.UUID) (any, error) {
		state, version, err := c.Load(r.Context(), r.PathValue("name"), ns, id)
		if err != nil || version == 0 {
			return nil, err
		}
		return map[string]any{"version": version, "state": state}, nil
	})
}

func (c *Client) docStream(w http.ResponseWriter, r *http.Request, load func(ns string, id uuid.UUID) (any, error)) {
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		apiError(w, http.StatusBadRequest, "namespace is required")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusBadRequest, "bad id")
		return
	}
	sse, ok := newSSE(w)
	if !ok {
		apiError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	var last []byte
	c.streamLoop(r.Context(), sse, func() error {
		doc, err := load(ns, id)
		if err != nil {
			return err
		}
		if doc == nil {
			return nil
		}
		raw, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		if bytes.Equal(raw, last) {
			return nil
		}
		last = raw
		return sse.event("", "change", json.RawMessage(raw))
	})
}
