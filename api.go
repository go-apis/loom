package loom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// HTTPHandler mounts the whole service as an HTTP API, driven entirely by
// the registry — no per-service handler code. Mount it behind whatever
// auth middleware the deployment uses:
//
//	mux.Handle("/loom/", http.StripPrefix("/loom", cli.HTTPHandler()))
//
// Routes:
//
//	POST /commands/{Command}            dispatch (body = command JSON)
//	GET  /entities/{Entity}?namespace=  filtered list (field=v, field.gte=v, order, limit, offset)
//	GET  /entities/{Entity}/{id}        one read-model row
//	GET  /records/{Record}/{id}         one state-of-record row
//	GET  /records/{Record}?namespace=   filtered list
//	GET  /aggregates/{Aggregate}/{id}   folded state + version
//	GET  /events                        log browser (type, aggregate_id, correlation_id, since, until, after_seq)
//	GET  /events/stats?since=           counts by event type
//	GET  /effects?status=               effect journal (running = in doubt if sustained)
//	POST /effects/resolve               settle an in-doubt effect (body: scope, key, result?)
//	GET  /dead_letters                  parked deliveries
//	POST /dead_letters/{id}/redrive     re-run one parked delivery
//	POST /shred                         delete a stream's @pii data key and files (body: namespace, id) — irreversible
//	POST /uploads                       open a resumable upload session (body: upload, namespace, id, name, content_type, size)
//	GET  /files?key=                    stream a stored file's bytes
//	GET  /stats                         ops health: outbox, dead letters, timers, effects
func (c *Client) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /commands/{name}", c.apiDispatch)
	mux.HandleFunc("POST /uploads", c.apiCreateUpload)
	mux.HandleFunc("GET /files", c.apiGetFile)
	mux.HandleFunc("GET /entities/{name}", c.apiList(c.QueryEntities))
	mux.HandleFunc("GET /entities/{name}/{id}", c.apiGetEntity)
	mux.HandleFunc("GET /records/{name}", c.apiList(c.QueryRecords))
	mux.HandleFunc("GET /records/{name}/{id}", c.apiGetRecord)
	mux.HandleFunc("GET /aggregates/{name}/{id}", c.apiGetAggregate)
	mux.HandleFunc("GET /events", c.apiEvents)
	mux.HandleFunc("GET /events/stats", c.apiEventStats)
	mux.HandleFunc("GET /events/stream", c.apiEventsStream)
	mux.HandleFunc("GET /entities/{name}/{id}/stream", c.apiEntityStream)
	mux.HandleFunc("GET /aggregates/{name}/{id}/stream", c.apiAggregateStream)
	mux.HandleFunc("GET /stats", c.apiStats)
	mux.HandleFunc("GET /effects", c.apiEffects)
	mux.HandleFunc("POST /effects/resolve", c.apiResolveEffect)
	mux.HandleFunc("GET /dead_letters", c.apiDeadLetters)
	mux.HandleFunc("POST /dead_letters/{id}/redrive", c.apiRedrive)
	mux.HandleFunc("POST /shred", c.apiShred)
	mux.HandleFunc("GET /console", c.apiConsole)
	mux.HandleFunc("GET /registry", c.apiRegistry)
	mux.HandleFunc("GET /runners", c.apiRunners)
	mux.HandleFunc("GET /timers", c.apiTimers)
	mux.HandleFunc("GET /batches", c.apiBatches)
	mux.HandleFunc("POST /batches", c.apiEnqueueBatch)
	mux.HandleFunc("GET /batches/{id}", c.apiGetBatch)
	mux.HandleFunc("GET /batches/{id}/failures", c.apiBatchFailures)
	mux.HandleFunc("GET /batches/{id}/stream", c.apiBatchStream)
	return mux
}

func (c *Client) apiEnqueueBatch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Namespace string `json:"namespace"`
		Commands  []struct {
			Command string          `json:"command"`
			Body    json.RawMessage `json:"body"`
		} `json:"commands"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Namespace == "" || len(body.Commands) == 0 {
		apiError(w, http.StatusBadRequest, "namespace and commands are required")
		return
	}
	cmds := make([]Command, 0, len(body.Commands))
	for i, item := range body.Commands {
		cmd, err := c.decodeCommand(item.Command, item.Body)
		if err != nil {
			apiError(w, http.StatusBadRequest, fmt.Sprintf("item %d: %v", i, err))
			return
		}
		cmds = append(cmds, cmd)
	}
	id, err := c.EnqueueBatch(r.Context(), body.Namespace, cmds)
	if err != nil {
		apiError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "total": len(cmds)})
}

func (c *Client) apiGetBatch(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusBadRequest, "bad id")
		return
	}
	b, err := c.GetBatch(r.Context(), id)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if b == nil {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (c *Client) apiBatchFailures(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusBadRequest, "bad id")
		return
	}
	failures, err := c.BatchFailures(r.Context(), id)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": orEmpty(failures)})
}

func (c *Client) apiBatchStream(w http.ResponseWriter, r *http.Request) {
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
		b, err := c.GetBatch(r.Context(), id)
		if err != nil || b == nil {
			return err
		}
		raw, err := json.Marshal(b)
		if err != nil {
			return err
		}
		if bytes.Equal(raw, last) {
			return nil
		}
		last = raw
		return sse.event("", "progress", json.RawMessage(raw))
	})
}

func (c *Client) apiCreateUpload(w http.ResponseWriter, r *http.Request) {
	var req UploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Origin = r.Header.Get("Origin")
	ctx := WithMeta(r.Context(), Metadata{
		CorrelationID: r.Header.Get("X-Correlation-Id"),
		Actor:         r.Header.Get("X-Actor"),
	})
	up, err := c.CreateUpload(ctx, req)
	if err != nil {
		apiError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, up)
}

func (c *Client) apiGetFile(w http.ResponseWriter, r *http.Request) {
	c.ServeFile(w, r, r.URL.Query().Get("key"))
}

// ServeFile streams one stored file with its content type, length, and
// original filename. It backs the service's GET /files and the
// gateway's — services can stay private; the gateway reaches storage
// through the same client. A later signed-URL upgrade can redirect from
// here without changing callers.
func (c *Client) ServeFile(w http.ResponseWriter, r *http.Request, key string) {
	if c.blobs == nil || key == "" {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	info, err := c.blobs.Stat(r.Context(), key)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if info == nil {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	body, err := c.blobs.Open(r.Context(), key)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer body.Close()
	if info.ContentType != "" {
		w.Header().Set("Content-Type", info.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if name := info.Metadata[metaName]; name != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	_, _ = io.Copy(w, body)
}

func (c *Client) apiDispatch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var cmd Command
	if _, def := c.reg.aggregateForCommand(name); def != nil {
		cmd = def.New()
	} else if _, rdef := c.reg.recordForCommand(name); rdef != nil {
		cmd = rdef.New()
	} else {
		apiError(w, http.StatusNotFound, fmt.Sprintf("unknown command %s", name))
		return
	}
	if err := json.NewDecoder(r.Body).Decode(cmd); err != nil {
		apiError(w, http.StatusBadRequest, "bad command body: "+err.Error())
		return
	}
	if ns, _ := cmd.CommandTarget(); ns == "" {
		apiError(w, http.StatusBadRequest, "command needs a namespace")
		return
	}

	ctx := WithMeta(r.Context(), Metadata{
		CorrelationID: r.Header.Get("X-Correlation-Id"),
		Actor:         r.Header.Get("X-Actor"),
	})
	err := c.Dispatch(ctx, cmd)
	var conflict *ConflictError
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "ok"})
	case errors.As(err, &conflict):
		apiError(w, http.StatusConflict, err.Error())
	default:
		apiError(w, http.StatusUnprocessableEntity, err.Error())
	}
}

func (c *Client) apiList(query func(ctx context.Context, name string, q Query) ([]Row, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q, err := queryFromURL(r)
		if err != nil {
			apiError(w, http.StatusBadRequest, err.Error())
			return
		}
		rows, err := query(r.Context(), r.PathValue("name"), q)
		if err != nil {
			apiError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": orEmpty(rows)})
	}
}

func (c *Client) apiGetEntity(w http.ResponseWriter, r *http.Request) {
	c.apiGetDoc(w, r, func(ns string, id uuid.UUID) (any, error) {
		state, err := c.Entity(r.Context(), r.PathValue("name"), ns, id)
		if state == nil {
			return nil, err
		}
		return state, err
	})
}

func (c *Client) apiGetRecord(w http.ResponseWriter, r *http.Request) {
	c.apiGetDoc(w, r, func(ns string, id uuid.UUID) (any, error) {
		name := r.PathValue("name")
		state, err := c.Record(r.Context(), name, ns, id)
		if err != nil || state == nil {
			return state, err
		}
		if def := c.reg.recordDef(name); def != nil {
			return RedactSecrets(state, def.StateSecret), nil
		}
		return state, nil
	})
}

func (c *Client) apiGetDoc(w http.ResponseWriter, r *http.Request, load func(ns string, id uuid.UUID) (any, error)) {
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
	doc, err := load(ns, id)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	if doc == nil {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func (c *Client) apiGetAggregate(w http.ResponseWriter, r *http.Request) {
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
	name := r.PathValue("name")
	state, version, err := c.Load(r.Context(), name, ns, id)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	if version == 0 {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	var out any = state
	if def := c.reg.aggregateDef(name); def != nil {
		out = RedactSecrets(state, def.StateSecret)
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "state": out})
}

func (c *Client) apiEvents(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Query()
	q := LogQuery{
		Namespace:     p.Get("namespace"),
		Type:          p.Get("type"),
		AggregateType: p.Get("aggregate_type"),
		AggregateID:   p.Get("aggregate_id"),
		CorrelationID: p.Get("correlation_id"),
		Ascending:     p.Get("order") == "asc",
	}
	if v := p.Get("after_seq"); v != "" {
		q.AfterSeq, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := p.Get("since"); v != "" {
		q.Since, _ = time.Parse(time.RFC3339, v)
	}
	if v := p.Get("until"); v != "" {
		q.Until, _ = time.Parse(time.RFC3339, v)
	}
	if v := p.Get("limit"); v != "" {
		q.Limit, _ = strconv.Atoi(v)
	}
	entries, err := c.QueryLog(r.Context(), q)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": orEmpty(entries)})
}

func (c *Client) apiEventStats(w http.ResponseWriter, r *http.Request) {
	var since time.Time
	if v := r.URL.Query().Get("since"); v != "" {
		since, _ = time.Parse(time.RFC3339, v)
	}
	stats, err := c.LogStats(r.Context(), since)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": stats})
}

func (c *Client) apiEffects(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	effects, err := c.Effects(r.Context(), r.URL.Query().Get("status"), limit)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": orEmpty(effects)})
}

func (c *Client) apiResolveEffect(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scope  string          `json:"scope"`
		Key    string          `json:"key"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Scope == "" || body.Key == "" {
		apiError(w, http.StatusBadRequest, "scope and key are required")
		return
	}
	if err := c.ResolveEffect(r.Context(), body.Scope, body.Key, body.Result); err != nil {
		apiError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resolved"})
}

func (c *Client) apiDeadLetters(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	letters, err := c.DeadLetters(r.Context(), limit)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": orEmpty(letters)})
}

func (c *Client) apiRedrive(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		apiError(w, http.StatusBadRequest, "bad id")
		return
	}
	if err := c.RedriveDeadLetter(r.Context(), id); err != nil {
		apiError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "redriven"})
}

func (c *Client) apiShred(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Namespace string    `json:"namespace"`
		ID        uuid.UUID `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Namespace == "" || body.ID == uuid.Nil {
		apiError(w, http.StatusBadRequest, "namespace and id are required")
		return
	}
	if err := c.Shred(r.Context(), body.Namespace, body.ID); err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "shredded"})
}

func (c *Client) apiStats(w http.ResponseWriter, r *http.Request) {
	depth, age, err := c.OutboxDepth(r.Context())
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var deadLetters, timers, effectsRunning, effectsFailed int64
	_ = c.db.QueryRow(r.Context(), `SELECT count(*) FROM loom_dead_letters WHERE service=$1`, c.reg.Service).Scan(&deadLetters)
	_ = c.db.QueryRow(r.Context(), `SELECT count(*) FROM loom_timers WHERE service=$1`, c.reg.Service).Scan(&timers)
	_ = c.db.QueryRow(r.Context(), `SELECT count(*) FILTER (WHERE status='running'), count(*) FILTER (WHERE status='failed') FROM loom_effects WHERE service=$1`, c.reg.Service).Scan(&effectsRunning, &effectsFailed)
	writeJSON(w, http.StatusOK, map[string]any{
		"service":            c.reg.Service,
		"outbox_depth":       depth,
		"outbox_oldest_secs": int64(age.Seconds()),
		"dead_letters":       deadLetters,
		"timers_pending":     timers,
		"effects_running":    effectsRunning,
		"effects_failed":     effectsFailed,
	})
}

// queryFromURL turns query params into a Query: reserved keys aside, every
// param is a filter — `status=shipped`, `total_cents.gte=1000`.
func queryFromURL(r *http.Request) (Query, error) {
	p := r.URL.Query()
	q := Query{
		Namespace: p.Get("namespace"),
		OrderBy:   p.Get("order"),
	}
	if v := p.Get("limit"); v != "" {
		q.Limit, _ = strconv.Atoi(v)
	}
	if v := p.Get("offset"); v != "" {
		q.Offset, _ = strconv.Atoi(v)
	}
	for key, values := range p {
		switch key {
		case "namespace", "order", "limit", "offset":
			continue
		}
		if len(values) == 0 {
			continue
		}
		field, op := key, ""
		if i := strings.LastIndex(key, "."); i > 0 {
			field, op = key[:i], key[i+1:]
		}
		q.Filters = append(q.Filters, Filter{Field: field, Op: op, Value: values[0]})
	}
	return q, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func apiError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func orEmpty[T any](in []T) []T {
	if in == nil {
		return []T{}
	}
	return in
}
