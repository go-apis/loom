package loom

import (
	_ "embed"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// The console is the ops UI every service carries: mount HTTPHandler and
// /console renders Overview, Design (the schema as the registry sees it,
// including reaction dispatch contracts), Events (log browser), and
// Issues (dead letters with redrive, in-doubt effects with resolve,
// runner lag, overdue timers). It is a single embedded page speaking to
// the sibling JSON endpoints — no build step, no external assets.

//go:embed console.html
var consoleHTML []byte

func (c *Client) apiConsole(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(consoleHTML)
}

// --- /registry: the Design tab's data ---

type registryDoc struct {
	Service     string          `json:"service"`
	Aggregates  []registryAgg   `json:"aggregates"`
	Records     []registryAgg   `json:"records"`
	Events      []registryEvent `json:"events"`
	Policies    []registryReact `json:"policies"`
	Processes   []registryReact `json:"processes"`
	Projections []registryProj  `json:"projections"`
}

type registryAgg struct {
	Name     string        `json:"name"`
	Snapshot int           `json:"snapshot,omitempty"`
	PII      []string      `json:"pii,omitempty"`
	Commands []registryCmd `json:"commands"`
}

type registryCmd struct {
	Name  string   `json:"name"`
	Emits []string `json:"emits"`
}

type registryEvent struct {
	Name    string   `json:"name"`
	Version int      `json:"version"`
	Publish bool     `json:"publish"`
	Service string   `json:"service,omitempty"` // set = consumed foreign event
	PII     []string `json:"pii,omitempty"`
}

type registryReact struct {
	Name    string        `json:"name"`
	Subs    []registrySub `json:"subs"`
	Effects []string      `json:"effects,omitempty"`
}

type registrySub struct {
	Event      string   `json:"event"`
	Dispatches []string `json:"dispatches,omitempty"`
}

type registryProj struct {
	Name   string   `json:"name"`
	Entity string   `json:"entity"`
	Events []string `json:"events"`
	PII    []string `json:"pii,omitempty"`
}

func (c *Client) apiRegistry(w http.ResponseWriter, r *http.Request) {
	doc := registryDoc{Service: c.reg.Service}
	for _, a := range c.reg.Aggregates {
		agg := registryAgg{Name: a.Name, Snapshot: a.SnapshotEvery, PII: a.StatePII}
		for _, cmd := range a.Commands {
			agg.Commands = append(agg.Commands, registryCmd{Name: cmd.Name, Emits: cmd.Emits})
		}
		doc.Aggregates = append(doc.Aggregates, agg)
	}
	for _, rec := range c.reg.Records {
		rd := registryAgg{Name: rec.Name, PII: rec.StatePII}
		for _, cmd := range rec.Commands {
			rd.Commands = append(rd.Commands, registryCmd{Name: cmd.Name, Emits: cmd.Emits})
		}
		doc.Records = append(doc.Records, rd)
	}
	for _, e := range c.reg.Events {
		doc.Events = append(doc.Events, registryEvent{Name: e.Name, Version: e.SchemaVersion, Publish: e.Publish, Service: e.Service, PII: e.PII})
	}
	react := func(defs []*ReactorDef) []registryReact {
		out := make([]registryReact, 0, len(defs))
		for _, p := range defs {
			rr := registryReact{Name: p.Name, Effects: p.Effects}
			for _, s := range p.Subs {
				rr.Subs = append(rr.Subs, registrySub{Event: s.Event, Dispatches: s.Dispatches})
			}
			// registries generated before Subs existed still render
			if len(rr.Subs) == 0 {
				for _, e := range p.Events {
					rr.Subs = append(rr.Subs, registrySub{Event: e})
				}
			}
			out = append(out, rr)
		}
		return out
	}
	doc.Policies = react(c.reg.Policies)
	doc.Processes = react(c.reg.Processes)
	for _, p := range c.reg.Projections {
		doc.Projections = append(doc.Projections, registryProj{Name: p.Name, Entity: p.Entity, Events: p.Events, PII: p.PII})
	}
	writeJSON(w, http.StatusOK, doc)
}

// --- /runners: checkpoint lag, the "is anything stuck" signal ---

type runnerStatus struct {
	Runner string `json:"runner"`
	// Kind: projection | process | process(bus) — bus consumers have no
	// checkpoint, they dedup instead.
	Kind      string     `json:"kind"`
	Seq       int64      `json:"seq"`
	Latest    int64      `json:"latest"`
	Lag       int64      `json:"lag"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

func (c *Client) apiRunners(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var latest int64
	_ = c.db.QueryRow(ctx, `SELECT coalesce(max(global_seq),0) FROM loom_events WHERE service=$1`, c.reg.Service).Scan(&latest)

	type cp struct {
		seq int64
		at  time.Time
	}
	cps := map[string]cp{}
	rows, err := c.db.Query(ctx, `SELECT runner, global_seq, updated_at FROM loom_checkpoints WHERE service=$1`, c.reg.Service)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for rows.Next() {
		var name string
		var v cp
		if err := rows.Scan(&name, &v.seq, &v.at); err != nil {
			rows.Close()
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
		cps[name] = v
	}
	rows.Close()

	out := []runnerStatus{}
	add := func(runner, kind string, checkpointed bool) {
		st := runnerStatus{Runner: runner, Kind: kind, Latest: latest}
		if v, ok := cps[runner]; ok {
			at := v.at
			st.Seq, st.UpdatedAt = v.seq, &at
		}
		if checkpointed {
			st.Lag = latest - st.Seq
		}
		out = append(out, st)
	}
	for _, p := range c.reg.Projections {
		add("projection:"+p.Name, "projection", true)
	}
	for _, p := range c.reg.Processes {
		local, foreign := c.splitSubscriptions(p)
		if len(local) > 0 {
			add("process:"+p.Name, "process", true)
		}
		if len(foreign) > 0 {
			add("process:"+p.Name+" (bus)", "process(bus)", false)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"latest": latest, "runners": out})
}

// --- /timers: pending schedule, oldest first ---

func (c *Client) apiTimers(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := c.db.Query(r.Context(), `
		SELECT key, namespace, command_type, fire_at, created_at FROM loom_timers
		WHERE service=$1 ORDER BY fire_at ASC LIMIT $2`, c.reg.Service, limit)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	type timer struct {
		Key         string    `json:"key"`
		Namespace   string    `json:"namespace"`
		CommandType string    `json:"command_type"`
		FireAt      time.Time `json:"fire_at"`
		CreatedAt   time.Time `json:"created_at"`
		Overdue     bool      `json:"overdue"`
	}
	out := []timer{}
	now := time.Now()
	for rows.Next() {
		var t timer
		if err := rows.Scan(&t.Key, &t.Namespace, &t.CommandType, &t.FireAt, &t.CreatedAt); err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
		// due timers fire within a poll tick; a minute late means stuck
		t.Overdue = now.Sub(t.FireAt) > time.Minute
		out = append(out, t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

// --- /batches: recent, newest first ---

func (c *Client) apiBatches(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 50
	}
	rows, err := c.db.Query(r.Context(), `
		SELECT id, namespace, status, total, done, failed, created_at, updated_at
		FROM loom_batches WHERE service=$1 ORDER BY created_at DESC LIMIT $2`,
		c.reg.Service, limit)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []Batch{}
	for rows.Next() {
		var b Batch
		var id uuid.UUID
		if err := rows.Scan(&id, &b.Namespace, &b.Status, &b.Total, &b.Done, &b.Failed, &b.CreatedAt, &b.UpdatedAt); err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
		b.ID = id
		out = append(out, b)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}
